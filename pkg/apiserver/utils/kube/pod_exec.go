package kube

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	httputil "net/http"
	"net/textproto"
	"net/url"
	"path"
	"strconv"
	"strings"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	utilexec "k8s.io/utils/exec"
)

var newRemoteExecutor = func(config *rest.Config, method string, execURL *url.URL) (remotecommand.Executor, error) {
	return remotecommand.NewSPDYExecutor(config, method, execURL)
}

const (
	syncExecOutputLimitBytes       = 1 << 20 // 1 MiB
	syncExecOutputTruncatedSuffix  = "\n...[truncated]"
	defaultArchiveContentTypeZip   = "application/zip"
	defaultArchiveFileExtZip       = ".zip"
	defaultArchiveFileExtMultipart = ".multipart"
	maxHardLinkCacheTotalBytes     = 64 << 20 // 64 MiB
)

var errArchivePathInvalid = errors.New("archive path is invalid")
var errArchivePathLookup = errors.New("archive path lookup failed")

var archivePathLookupErrorMarkers = []string{
	"no such file or directory",
	"cannot stat",
	"can't stat",
	"cannot access",
	"cannot open",
	"can't open",
	"permission denied",
}

// PodExecResult captures synchronous shell execution output from a Pod.
type PodExecResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

type PodShellStreamEventType string

const (
	PodShellStreamEventStdout PodShellStreamEventType = "stdout"
	PodShellStreamEventStderr PodShellStreamEventType = "stderr"
	PodShellStreamEventExit   PodShellStreamEventType = "exit"
	PodShellStreamEventError  PodShellStreamEventType = "error"
)

// PodShellStreamEvent represents a shell stream event from Pod exec.
type PodShellStreamEvent struct {
	Type      PodShellStreamEventType
	Chunk     string
	ExitCode  int
	Succeeded bool
	Message   string
}

// PodPathArchiveStream represents a component file archive stream.
type PodPathArchiveStream struct {
	Reader      io.ReadCloser
	FileName    string
	ContentType string
}

func (s *PodPathArchiveStream) Close() error {
	if s == nil || s.Reader == nil {
		return nil
	}
	return s.Reader.Close()
}

// Succeeded reports whether the command exited with code 0.
func (r PodExecResult) Succeeded() bool {
	return r.ExitCode == 0
}

// ExecPodShellScript executes a shell script in a Pod and captures stdout, stderr, and exit code.
func ExecPodShellScript(ctx context.Context, client kubernetes.Interface, restConfig *rest.Config, namespace, podName, container, script string) (*PodExecResult, error) {
	script = strings.TrimSpace(script)
	if script == "" {
		return nil, fmt.Errorf("shell script is empty")
	}

	stdout := newCappedBuffer(syncExecOutputLimitBytes)
	stderr := newCappedBuffer(syncExecOutputLimitBytes)
	result := &PodExecResult{}
	err := streamPodCommand(ctx, restConfig, namespace, podName, container, []string{"/bin/sh", "-c", script}, nil, &stdout, &stderr, false)
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()
	if err == nil {
		return result, nil
	}

	var exitErr utilexec.ExitError
	if errors.As(err, &exitErr) {
		result.ExitCode = exitErr.ExitStatus()
		return result, nil
	}
	return nil, wrapPodCommandError("execute shell script", err, result.Stderr)
}

// StreamPodShellScript executes a shell script in a Pod and returns stream events.
func StreamPodShellScript(ctx context.Context, client kubernetes.Interface, restConfig *rest.Config, namespace, podName, container, script string) (<-chan PodShellStreamEvent, error) {
	script = strings.TrimSpace(script)
	if script == "" {
		return nil, fmt.Errorf("shell script is empty")
	}
	events := make(chan PodShellStreamEvent, 32)
	go func() {
		defer close(events)
		emit := func(event PodShellStreamEvent) bool {
			select {
			case <-ctx.Done():
				return false
			case events <- event:
				return true
			}
		}
		stdoutWriter := &podStreamEventWriter{
			ctx:       ctx,
			eventType: PodShellStreamEventStdout,
			emit:      emit,
		}
		stderrWriter := &podStreamEventWriter{
			ctx:       ctx,
			eventType: PodShellStreamEventStderr,
			emit:      emit,
		}
		err := streamPodCommand(ctx, restConfig, namespace, podName, container, []string{"/bin/sh", "-c", script}, nil, stdoutWriter, stderrWriter, false)
		if err == nil {
			_ = emit(PodShellStreamEvent{
				Type:      PodShellStreamEventExit,
				ExitCode:  0,
				Succeeded: true,
			})
			return
		}
		var exitErr utilexec.ExitError
		if errors.As(err, &exitErr) {
			_ = emit(PodShellStreamEvent{
				Type:      PodShellStreamEventExit,
				ExitCode:  exitErr.ExitStatus(),
				Succeeded: false,
			})
			return
		}
		_ = emit(PodShellStreamEvent{
			Type:    PodShellStreamEventError,
			Message: fmt.Sprintf("execute shell script: %v", err),
		})
	}()
	return events, nil
}

// ArchivePodPathAsZip archives a Pod file or directory into a zip stream.
func ArchivePodPathAsZip(ctx context.Context, client kubernetes.Interface, restConfig *rest.Config, namespace, podName, container, targetPath string) (*PodPathArchiveStream, error) {
	archiveDir, archiveBase, archiveName, err := normalizeArchivePath(targetPath)
	if err != nil {
		return nil, err
	}

	hasTar, err := podHasTar(ctx, restConfig, namespace, podName, container)
	if err != nil {
		return nil, err
	}
	if hasTar {
		reader := archivePodPathWithTar(ctx, restConfig, namespace, podName, container, archiveDir, archiveBase)
		return &PodPathArchiveStream{
			Reader:      reader,
			FileName:    archiveName + defaultArchiveFileExtZip,
			ContentType: defaultArchiveContentTypeZip,
		}, nil
	}
	reader, contentType := archivePodPathAsMultipart(ctx, restConfig, namespace, podName, container, archiveDir, archiveBase)
	return &PodPathArchiveStream{
		Reader:      reader,
		FileName:    archiveName + defaultArchiveFileExtMultipart,
		ContentType: contentType,
	}, nil
}

func archivePodPathWithTar(ctx context.Context, restConfig *rest.Config, namespace, podName, container, archiveDir, archiveBase string) io.ReadCloser {

	tarReader, tarWriter := io.Pipe()
	zipReader, zipWriter := io.Pipe()

	go func() {
		var stderr bytes.Buffer
		err := streamPodCommand(ctx, restConfig, namespace, podName, container, []string{"tar", "-C", archiveDir, "-cf", "-", "--", archiveBase}, nil, tarWriter, &stderr, false)
		if err != nil {
			_ = tarWriter.CloseWithError(wrapArchivePathCommandError("archive pod path", err, stderr.String()))
			return
		}
		_ = tarWriter.Close()
	}()

	go func() {
		resolveHardLinkContent := func(targetName string, writer io.Writer) error {
			targetName, err := sanitizeArchiveEntryName(targetName)
			if err != nil {
				return err
			}
			if targetName == "" {
				return fmt.Errorf("hard link target name is empty")
			}
			if err := writePodFileContentToWriter(ctx, restConfig, namespace, podName, container, archiveDir, targetName, writer); err != nil {
				return err
			}
			return nil
		}
		if err := writeZipFromTarStream(tarReader, zipWriter, resolveHardLinkContent); err != nil {
			_ = tarReader.CloseWithError(err)
			_ = zipWriter.CloseWithError(err)
			return
		}
		_ = zipWriter.Close()
	}()

	return zipReader
}

func archivePodPathAsMultipart(ctx context.Context, restConfig *rest.Config, namespace, podName, container, archiveDir, archiveBase string) (io.ReadCloser, string) {
	reader, writer := io.Pipe()
	multipartWriter := multipart.NewWriter(writer)
	contentType := "multipart/mixed; boundary=" + multipartWriter.Boundary()
	go func() {
		if err := writeMultipartFromPodPath(ctx, restConfig, namespace, podName, container, archiveDir, archiveBase, multipartWriter); err != nil {
			_ = writer.CloseWithError(err)
			return
		}
		if err := multipartWriter.Close(); err != nil {
			_ = writer.CloseWithError(fmt.Errorf("close multipart writer: %w", err))
			return
		}
		_ = writer.Close()
	}()
	return reader, contentType
}

func writeMultipartFromPodPath(ctx context.Context, restConfig *rest.Config, namespace, podName, container, archiveDir, archiveBase string, multipartWriter *multipart.Writer) error {
	listCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	findReader, findWriter := io.Pipe()
	defer func() { _ = findReader.Close() }()
	findErrChan := make(chan error, 1)
	go func() {
		var stderr bytes.Buffer
		command := []string{
			"/bin/sh",
			"-c",
			`cd "$1" && find "./$2" -type f`,
			"sh",
			archiveDir,
			archiveBase,
		}
		err := streamPodCommand(listCtx, restConfig, namespace, podName, container, command, nil, findWriter, &stderr, false)
		if err != nil {
			wrapped := wrapArchivePathCommandError("list pod files", err, stderr.String())
			_ = findWriter.CloseWithError(wrapped)
			findErrChan <- wrapped
			return
		}
		_ = findWriter.Close()
		findErrChan <- nil
	}()

	scanner := bufio.NewScanner(findReader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1*1024*1024)
	for scanner.Scan() {
		rawEntry := strings.TrimSuffix(scanner.Text(), "\r")
		if rawEntry == "" {
			continue
		}
		relativePath, err := sanitizeArchiveEntryName(strings.TrimPrefix(rawEntry, "./"))
		if err != nil {
			cancel()
			return err
		}
		if relativePath == "" {
			continue
		}
		partWriter, err := createMultipartFilePart(multipartWriter, relativePath)
		if err != nil {
			cancel()
			return err
		}
		if err := writePodFileContentToWriter(ctx, restConfig, namespace, podName, container, archiveDir, rawEntry, partWriter); err != nil {
			cancel()
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		cancel()
		return fmt.Errorf("read file list stream: %w", err)
	}
	if err := <-findErrChan; err != nil {
		return err
	}
	return nil
}

func createMultipartFilePart(multipartWriter *multipart.Writer, relativePath string) (io.Writer, error) {
	headers := make(textproto.MIMEHeader)
	headers.Set("Content-Type", "application/octet-stream")
	headers.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", path.Base(relativePath)))
	headers.Set("X-Eruun-Path", relativePath)
	partWriter, err := multipartWriter.CreatePart(headers)
	if err != nil {
		return nil, fmt.Errorf("create multipart part for %s: %w", relativePath, err)
	}
	return partWriter, nil
}

func writePodFileContentToWriter(ctx context.Context, restConfig *rest.Config, namespace, podName, container, archiveDir, filePath string, writer io.Writer) error {
	var stderr bytes.Buffer
	command := []string{
		"/bin/sh",
		"-c",
		`cd "$1" && cat "$2"`,
		"sh",
		archiveDir,
		filePath,
	}
	if err := streamPodCommand(ctx, restConfig, namespace, podName, container, command, nil, writer, &stderr, false); err != nil {
		return wrapArchivePathCommandError("read pod file content", err, stderr.String())
	}
	return nil
}

func podHasTar(ctx context.Context, restConfig *rest.Config, namespace, podName, container string) (bool, error) {
	var stderr bytes.Buffer
	err := streamPodCommand(ctx, restConfig, namespace, podName, container, []string{"/bin/sh", "-c", "command -v tar >/dev/null 2>&1"}, nil, io.Discard, &stderr, false)
	if err == nil {
		return true, nil
	}
	var exitErr utilexec.ExitError
	if errors.As(err, &exitErr) {
		return false, nil
	}
	return false, wrapPodCommandError("probe tar command", err, stderr.String())
}

func streamPodCommand(ctx context.Context, restConfig *rest.Config, namespace, podName, container string, command []string, stdin io.Reader, stdout, stderr io.Writer, tty bool) error {
	if restConfig == nil {
		return fmt.Errorf("kube config is nil")
	}
	if strings.TrimSpace(namespace) == "" {
		return fmt.Errorf("namespace is empty")
	}
	if strings.TrimSpace(podName) == "" {
		return fmt.Errorf("pod name is empty")
	}
	if len(command) == 0 {
		return fmt.Errorf("command is empty")
	}
	execURL, err := buildPodExecURL(restConfig, namespace, podName, container, command, stdin != nil, stdout != nil, stderr != nil, tty)
	if err != nil {
		return err
	}
	executor, err := newRemoteExecutor(restConfig, httputil.MethodPost, execURL)
	if err != nil {
		return fmt.Errorf("create remote executor: %w", err)
	}
	if err := executor.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:  stdin,
		Stdout: stdout,
		Stderr: stderr,
		Tty:    tty,
	}); err != nil {
		return err
	}
	return nil
}

func buildPodExecURL(restConfig *rest.Config, namespace, podName, container string, command []string, stdin, stdout, stderr, tty bool) (*url.URL, error) {
	baseURL, err := url.Parse(restConfig.Host)
	if err != nil {
		return nil, fmt.Errorf("parse kube host: %w", err)
	}
	apiPath := strings.TrimSpace(restConfig.APIPath)
	if apiPath == "" {
		apiPath = "/api"
	}
	apiBasePath := resolveExecAPIBasePath(baseURL.Path, apiPath)
	baseURL.Path = path.Join(apiBasePath, "v1", "namespaces", namespace, "pods", podName, "exec")

	query := baseURL.Query()
	if container != "" {
		query.Set("container", container)
	}
	for _, cmd := range command {
		query.Add("command", cmd)
	}
	query.Set("stdin", strconv.FormatBool(stdin))
	query.Set("stdout", strconv.FormatBool(stdout))
	query.Set("stderr", strconv.FormatBool(stderr))
	query.Set("tty", strconv.FormatBool(tty))
	baseURL.RawQuery = query.Encode()
	return baseURL, nil
}

func resolveExecAPIBasePath(hostPath, apiPath string) string {
	hostPath = strings.TrimSpace(hostPath)
	if hostPath == "" {
		hostPath = "/"
	}
	if !strings.HasPrefix(hostPath, "/") {
		hostPath = "/" + hostPath
	}
	hostPath = path.Clean(hostPath)

	apiPath = strings.TrimSpace(apiPath)
	if apiPath == "" {
		apiPath = "/api"
	}
	if !strings.HasPrefix(apiPath, "/") {
		apiPath = "/" + apiPath
	}
	apiPath = path.Clean(apiPath)

	if hostPath == "/" {
		return apiPath
	}
	if apiPath == hostPath || strings.HasPrefix(apiPath, hostPath+"/") {
		return apiPath
	}
	if strings.HasSuffix(hostPath, apiPath) {
		return hostPath
	}
	return path.Join(hostPath, strings.TrimPrefix(apiPath, "/"))
}

func normalizeArchivePath(targetPath string) (string, string, string, error) {
	cleaned := path.Clean(strings.TrimSpace(targetPath))
	if cleaned == "" || cleaned == "." || cleaned == "/" || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", "", "", errArchivePathInvalid
	}
	archiveDir := path.Dir(cleaned)
	if archiveDir == "" {
		archiveDir = "."
	}
	archiveBase := path.Base(cleaned)
	archiveName := sanitizeArchiveFileName(archiveBase)
	if archiveName == "" {
		archiveName = "archive"
	}
	return archiveDir, archiveBase, archiveName, nil
}

func sanitizeArchiveFileName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")
	name = strings.Trim(name, ".")
	return name
}

func writeZipFromTarStream(tarReader io.Reader, zipWriter io.Writer, resolveHardLinkContent func(string, io.Writer) error) error {
	tr := tar.NewReader(tarReader)
	zw := zip.NewWriter(zipWriter)
	hardLinkCache := newTarHardLinkCache(maxHardLinkCacheTotalBytes)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				return zw.Close()
			}
			return fmt.Errorf("read tar stream: %w", err)
		}
		if err := copyTarEntryToZip(zw, tr, hdr, hardLinkCache, resolveHardLinkContent); err != nil {
			return err
		}
	}
}

func copyTarEntryToZip(zw *zip.Writer, tr *tar.Reader, hdr *tar.Header, hardLinkCache *tarHardLinkCache, resolveHardLinkContent func(string, io.Writer) error) error {
	if hdr == nil {
		return nil
	}
	name, err := sanitizeArchiveEntryName(hdr.Name)
	if err != nil {
		return err
	}
	if name == "" {
		return nil
	}

	switch hdr.Typeflag {
	case tar.TypeXHeader, tar.TypeXGlobalHeader:
		return nil
	case tar.TypeDir:
		if !strings.HasSuffix(name, "/") {
			name += "/"
		}
		return createZipDirectory(zw, hdr, name)
	case tar.TypeReg, tar.TypeRegA:
		return copyTarRegularEntryToZip(zw, tr, hdr, name, hardLinkCache)
	case tar.TypeSymlink:
		return createZipFile(zw, strings.NewReader(hdr.Linkname), hdr, name)
	case tar.TypeLink:
		return copyTarHardLinkToZip(zw, hdr, name, hardLinkCache, resolveHardLinkContent)
	default:
		return fmt.Errorf("unsupported tar entry type %q for %s", string([]byte{hdr.Typeflag}), name)
	}
}

func copyTarRegularEntryToZip(zw *zip.Writer, tr *tar.Reader, hdr *tar.Header, name string, hardLinkCache *tarHardLinkCache) error {
	writer, err := createZipFileWriter(zw, hdr, name)
	if err != nil {
		return err
	}

	var cacheBuffer bytes.Buffer
	copyTarget := io.Writer(writer)
	cached := false
	if hardLinkCache != nil && hardLinkCache.CanStore(hdr.Size) {
		copyTarget = io.MultiWriter(writer, &cacheBuffer)
		cached = true
	}
	if _, err := io.Copy(copyTarget, tr); err != nil {
		return fmt.Errorf("write zip file %s: %w", name, err)
	}
	if hardLinkCache != nil && cached {
		hardLinkCache.Add(name, cacheBuffer.Bytes())
	}
	return nil
}

func copyTarHardLinkToZip(zw *zip.Writer, hdr *tar.Header, name string, hardLinkCache *tarHardLinkCache, resolveHardLinkContent func(string, io.Writer) error) error {
	targetName, err := sanitizeArchiveEntryName(hdr.Linkname)
	if err != nil {
		return err
	}
	if targetName == "" {
		return fmt.Errorf("hard link target is empty for %s", name)
	}
	var targetContent []byte
	exists := false
	if hardLinkCache != nil {
		targetContent, exists = hardLinkCache.Get(targetName)
	}
	if !exists && resolveHardLinkContent != nil {
		writer, err := createZipFileWriter(zw, hdr, name)
		if err != nil {
			return err
		}
		if err := resolveHardLinkContent(targetName, writer); err != nil {
			return err
		}
		return nil
	}
	if !exists {
		return fmt.Errorf("hard link target %q content unavailable for %s", targetName, name)
	}
	return createZipFile(zw, bytes.NewReader(targetContent), hdr, name)
}

type tarHardLinkCache struct {
	maxBytes int64
	used     int64
	order    []string
	entries  map[string][]byte
}

func newTarHardLinkCache(maxBytes int64) *tarHardLinkCache {
	return &tarHardLinkCache{
		maxBytes: maxBytes,
		order:    make([]string, 0, 16),
		entries:  make(map[string][]byte),
	}
}

func (c *tarHardLinkCache) CanStore(size int64) bool {
	if c == nil {
		return false
	}
	return size >= 0 && size <= c.maxBytes && c.maxBytes > 0
}

func (c *tarHardLinkCache) Add(name string, content []byte) {
	if c == nil || strings.TrimSpace(name) == "" {
		return
	}
	size := int64(len(content))
	if !c.CanStore(size) {
		return
	}
	if existing, exists := c.entries[name]; exists {
		c.used -= int64(len(existing))
		delete(c.entries, name)
		c.removeFromOrder(name)
	}
	for c.used+size > c.maxBytes && len(c.order) > 0 {
		oldest := c.order[0]
		c.order = c.order[1:]
		if evicted, exists := c.entries[oldest]; exists {
			delete(c.entries, oldest)
			c.used -= int64(len(evicted))
		}
	}
	if c.used+size > c.maxBytes {
		return
	}
	c.entries[name] = append([]byte(nil), content...)
	c.order = append(c.order, name)
	c.used += size
}

func (c *tarHardLinkCache) Get(name string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	content, exists := c.entries[name]
	if !exists {
		return nil, false
	}
	return content, true
}

func (c *tarHardLinkCache) removeFromOrder(name string) {
	filtered := c.order[:0]
	for _, item := range c.order {
		if item != name {
			filtered = append(filtered, item)
		}
	}
	c.order = filtered
}

func createZipDirectory(zw *zip.Writer, hdr *tar.Header, name string) error {
	zipHeader, err := zip.FileInfoHeader(hdr.FileInfo())
	if err != nil {
		return fmt.Errorf("create zip header for %s: %w", name, err)
	}
	zipHeader.Name = name
	zipHeader.Method = zip.Store
	if _, err := zw.CreateHeader(zipHeader); err != nil {
		return fmt.Errorf("create zip directory %s: %w", name, err)
	}
	return nil
}

func createZipFile(zw *zip.Writer, reader io.Reader, hdr *tar.Header, name string) error {
	writer, err := createZipFileWriter(zw, hdr, name)
	if err != nil {
		return err
	}
	if _, err := io.Copy(writer, reader); err != nil {
		return fmt.Errorf("write zip file %s: %w", name, err)
	}
	return nil
}

func createZipFileWriter(zw *zip.Writer, hdr *tar.Header, name string) (io.Writer, error) {
	zipHeader, err := zip.FileInfoHeader(hdr.FileInfo())
	if err != nil {
		return nil, fmt.Errorf("create zip header for %s: %w", name, err)
	}
	zipHeader.Name = name
	zipHeader.Method = zip.Deflate
	writer, err := zw.CreateHeader(zipHeader)
	if err != nil {
		return nil, fmt.Errorf("create zip file %s: %w", name, err)
	}
	return writer, nil
}

func sanitizeArchiveEntryName(name string) (string, error) {
	cleaned := strings.ReplaceAll(strings.TrimSpace(name), "\\", "/")
	if cleaned == "" {
		return "", nil
	}
	for _, segment := range strings.Split(cleaned, "/") {
		if segment == ".." {
			return "", fmt.Errorf("archive entry path escapes destination: %s", name)
		}
	}
	cleaned = strings.TrimPrefix(path.Clean("/"+cleaned), "/")
	if cleaned == "" || cleaned == "." {
		return "", nil
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("archive entry path escapes destination: %s", name)
	}
	return cleaned, nil
}

func wrapPodCommandError(action string, err error, stderr string) error {
	stderr = strings.TrimSpace(stderr)
	if stderr == "" {
		return fmt.Errorf("%s: %w", action, err)
	}
	return fmt.Errorf("%s: %w: %s", action, err, stderr)
}

func wrapArchivePathCommandError(action string, err error, stderr string) error {
	wrapped := wrapPodCommandError(action, err, stderr)
	if isArchivePathLookupFailure(err, stderr) {
		return fmt.Errorf("%w: %w", errArchivePathLookup, wrapped)
	}
	return wrapped
}

func isArchivePathLookupFailure(err error, stderr string) bool {
	if err == nil {
		return false
	}
	var exitErr utilexec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	return containsArchivePathLookupMarker(stderr)
}

func containsArchivePathLookupMarker(text string) bool {
	lowered := strings.ToLower(strings.TrimSpace(text))
	if lowered == "" {
		return false
	}
	for _, marker := range archivePathLookupErrorMarkers {
		if strings.Contains(lowered, marker) {
			return true
		}
	}
	return false
}

func IsArchivePathInvalidError(err error) bool {
	return errors.Is(err, errArchivePathInvalid)
}

func IsArchivePathLookupError(err error) bool {
	if errors.Is(err, errArchivePathLookup) {
		return true
	}
	if err == nil {
		return false
	}
	lowered := strings.ToLower(err.Error())
	if !strings.Contains(lowered, "archive pod path") &&
		!strings.Contains(lowered, "list pod files") &&
		!strings.Contains(lowered, "read pod file content") {
		return false
	}
	return containsArchivePathLookupMarker(lowered)
}

type podStreamEventWriter struct {
	ctx       context.Context
	eventType PodShellStreamEventType
	emit      func(PodShellStreamEvent) bool
}

func (w *podStreamEventWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	chunk := append([]byte(nil), data...)
	if !w.emit(PodShellStreamEvent{
		Type:  w.eventType,
		Chunk: string(chunk),
	}) {
		if err := w.ctx.Err(); err != nil {
			return 0, err
		}
		return 0, io.ErrClosedPipe
	}
	return len(data), nil
}

type cappedBuffer struct {
	maxBytes  int
	buffer    bytes.Buffer
	truncated bool
}

func newCappedBuffer(maxBytes int) cappedBuffer {
	return cappedBuffer{maxBytes: maxBytes}
}

func (b *cappedBuffer) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	if b.maxBytes <= 0 {
		b.truncated = true
		return len(data), nil
	}
	remaining := b.maxBytes - b.buffer.Len()
	switch {
	case remaining <= 0:
		b.truncated = true
	case len(data) <= remaining:
		_, _ = b.buffer.Write(data)
	default:
		_, _ = b.buffer.Write(data[:remaining])
		b.truncated = true
	}
	return len(data), nil
}

func (b *cappedBuffer) String() string {
	out := b.buffer.String()
	if !b.truncated {
		return out
	}
	return out + syncExecOutputTruncatedSuffix
}

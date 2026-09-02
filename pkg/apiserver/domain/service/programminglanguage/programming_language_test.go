package programminglanguage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestNewProgrammingLanguageServiceWithRepositoryRequiresDependency(t *testing.T) {
	service, err := NewProgrammingLanguageServiceWithRepository(nil)
	require.EqualError(t, err, "create programming language service: repository is nil")
	require.Nil(t, service)

	repo := newFakeProgrammingLanguageRepo()
	service, err = NewProgrammingLanguageServiceWithRepository(repo)
	require.NoError(t, err)
	require.NotNil(t, service)
}

func TestProgrammingLanguageServiceCreateAndList(t *testing.T) {
	repo := newFakeProgrammingLanguageRepo()
	svc := &programmingLanguageServiceImpl{LanguageRepo: repo}

	enabled := true
	created, err := svc.Create(context.Background(), CreateProgrammingLanguageCommand{
		Name:    ".NET",
		Version: "8.0",
		Enabled: &enabled,
		CPUReq:  "100m",
		MemReq:  "0Mi",
	})

	require.NoError(t, err)
	require.NotEmpty(t, created.ID)
	require.Equal(t, "net", created.Code)
	require.NotNil(t, created.Enabled)
	require.True(t, *created.Enabled)

	languages, err := svc.List(context.Background())
	require.NoError(t, err)
	require.Len(t, languages, 1)
	require.Equal(t, created.ID, languages[0].ID)
}

func TestProgrammingLanguageServiceCreateRejectsDuplicateCodeVersion(t *testing.T) {
	repo := newFakeProgrammingLanguageRepo()
	svc := &programmingLanguageServiceImpl{LanguageRepo: repo}
	enabled := true
	req := CreateProgrammingLanguageCommand{
		Name:    "Golang",
		Version: "1.24",
		Enabled: &enabled,
	}

	_, err := svc.Create(context.Background(), req)
	require.NoError(t, err)

	_, err = svc.Create(context.Background(), req)
	require.ErrorIs(t, err, bcode.ErrProgrammingLanguageExists)
}

func TestProgrammingLanguageServiceCreateDoesNotMergeLanguageAliases(t *testing.T) {
	repo := newFakeProgrammingLanguageRepo()
	svc := &programmingLanguageServiceImpl{LanguageRepo: repo}
	enabled := true

	goLanguage, err := svc.Create(context.Background(), CreateProgrammingLanguageCommand{
		Name:    "Go",
		Version: "1.24",
		Enabled: &enabled,
	})
	require.NoError(t, err)

	golangLanguage, err := svc.Create(context.Background(), CreateProgrammingLanguageCommand{
		Name:    "Golang",
		Version: "1.24",
		Enabled: &enabled,
	})
	require.NoError(t, err)

	require.Equal(t, "go", goLanguage.Code)
	require.Equal(t, "golang", golangLanguage.Code)
}

func TestProgrammingLanguageServiceCreatePreservesSymbolDistinctions(t *testing.T) {
	repo := newFakeProgrammingLanguageRepo()
	svc := &programmingLanguageServiceImpl{LanguageRepo: repo}
	enabled := true

	cLanguage, err := svc.Create(context.Background(), CreateProgrammingLanguageCommand{
		Name:    "C",
		Version: "12",
		Enabled: &enabled,
	})
	require.NoError(t, err)

	csharpLanguage, err := svc.Create(context.Background(), CreateProgrammingLanguageCommand{
		Name:    "C#",
		Version: "12",
		Enabled: &enabled,
	})
	require.NoError(t, err)

	cppLanguage, err := svc.Create(context.Background(), CreateProgrammingLanguageCommand{
		Name:    "C++",
		Version: "12",
		Enabled: &enabled,
	})
	require.NoError(t, err)

	require.Equal(t, "c", cLanguage.Code)
	require.Equal(t, "c-23", csharpLanguage.Code)
	require.Equal(t, "c-2b-2b", cppLanguage.Code)
}

func TestProgrammingLanguageServiceCreateRejectsDuplicateSymbolLanguage(t *testing.T) {
	repo := newFakeProgrammingLanguageRepo()
	svc := &programmingLanguageServiceImpl{LanguageRepo: repo}
	enabled := true
	req := CreateProgrammingLanguageCommand{
		Name:    "C#",
		Version: "12",
		Enabled: &enabled,
	}

	created, err := svc.Create(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "c-23", created.Code)

	_, err = svc.Create(context.Background(), req)
	require.ErrorIs(t, err, bcode.ErrProgrammingLanguageExists)
}

func TestProgrammingLanguageServiceCreateEncodesSymbolOnlyName(t *testing.T) {
	repo := newFakeProgrammingLanguageRepo()
	svc := &programmingLanguageServiceImpl{LanguageRepo: repo}
	enabled := true

	created, err := svc.Create(context.Background(), CreateProgrammingLanguageCommand{
		Name:    "++",
		Version: "1.24",
		Enabled: &enabled,
	})

	require.NoError(t, err)
	require.Equal(t, "2b-2b", created.Code)
}

func TestProgrammingLanguageServiceGetAndDeleteMissing(t *testing.T) {
	svc := &programmingLanguageServiceImpl{LanguageRepo: newFakeProgrammingLanguageRepo()}

	_, err := svc.Get(context.Background(), "missing")
	require.ErrorIs(t, err, bcode.ErrProgrammingLanguageNotFound)

	err = svc.Delete(context.Background(), "missing")
	require.ErrorIs(t, err, bcode.ErrProgrammingLanguageNotFound)
}

func TestProgrammingLanguageServiceUpdateKeepsCodeImmutable(t *testing.T) {
	repo := newFakeProgrammingLanguageRepo()
	svc := &programmingLanguageServiceImpl{LanguageRepo: repo}
	repo.mustStore(t, &model.ProgrammingLanguage{
		ID:      "lang-1",
		Code:    "golang",
		Name:    "Golang",
		Version: "1.24",
		Enabled: boolPtr(true),
	})

	enabled := false
	name := "Go"
	version := "1.25"
	updated, err := svc.Update(context.Background(), "lang-1", UpdateProgrammingLanguageCommand{
		Name:    &name,
		Version: &version,
		Enabled: &enabled,
	})

	require.NoError(t, err)
	require.Equal(t, "golang", updated.Code)
	require.Equal(t, "Go", updated.Name)
	require.Equal(t, "1.25", updated.Version)
	require.NotNil(t, updated.Enabled)
	require.False(t, *updated.Enabled)
}

func TestProgrammingLanguageServiceUpdateClearsResourceRequests(t *testing.T) {
	repo := newFakeProgrammingLanguageRepo()
	svc := &programmingLanguageServiceImpl{LanguageRepo: repo}
	repo.mustStore(t, &model.ProgrammingLanguage{
		ID:      "lang-1",
		Code:    "golang",
		Name:    "Golang",
		Version: "1.24",
		Enabled: boolPtr(true),
		CPUReq:  "100m",
		MemReq:  "512Mi",
	})

	cpuReq := ""
	memReq := ""
	updated, err := svc.Update(context.Background(), "lang-1", UpdateProgrammingLanguageCommand{
		CPUReq: &cpuReq,
		MemReq: &memReq,
	})

	require.NoError(t, err)
	require.Empty(t, updated.CPUReq)
	require.Empty(t, updated.MemReq)
	require.Empty(t, repo.items["lang-1"].CPUReq)
	require.Empty(t, repo.items["lang-1"].MemReq)
}

func TestProgrammingLanguageServiceUpdatePreservesResourceRequestsWhenOmitted(t *testing.T) {
	repo := newFakeProgrammingLanguageRepo()
	svc := &programmingLanguageServiceImpl{LanguageRepo: repo}
	repo.mustStore(t, &model.ProgrammingLanguage{
		ID:      "lang-1",
		Code:    "golang",
		Name:    "Golang",
		Version: "1.24",
		Enabled: boolPtr(true),
		CPUReq:  "100m",
		MemReq:  "512Mi",
	})

	name := "Go"
	updated, err := svc.Update(context.Background(), "lang-1", UpdateProgrammingLanguageCommand{
		Name: &name,
	})

	require.NoError(t, err)
	require.Equal(t, "100m", updated.CPUReq)
	require.Equal(t, "512Mi", updated.MemReq)
	require.Equal(t, "100m", repo.items["lang-1"].CPUReq)
	require.Equal(t, "512Mi", repo.items["lang-1"].MemReq)
}

func TestProgrammingLanguageServiceUpdateReturnsSyncedUpdateTime(t *testing.T) {
	repo := newFakeProgrammingLanguageRepo()
	svc := &programmingLanguageServiceImpl{LanguageRepo: repo}
	oldTime := time.Unix(1600000000, 0)
	newTime := time.Unix(1700000000, 0)
	repo.updateTime = newTime
	repo.mustStore(t, &model.ProgrammingLanguage{
		ID:        "lang-1",
		Code:      "golang",
		Name:      "Golang",
		Version:   "1.24",
		Enabled:   boolPtr(true),
		BaseModel: model.BaseModel{UpdateTime: oldTime},
	})

	name := "Go"
	updated, err := svc.Update(context.Background(), "lang-1", UpdateProgrammingLanguageCommand{
		Name: &name,
	})

	require.NoError(t, err)
	require.Equal(t, newTime, updated.UpdateTime)
	require.Equal(t, newTime, repo.items["lang-1"].UpdateTime)
}

func TestProgrammingLanguageServiceUpdateRejectsDuplicateVersion(t *testing.T) {
	repo := newFakeProgrammingLanguageRepo()
	svc := &programmingLanguageServiceImpl{LanguageRepo: repo}
	repo.mustStore(t, &model.ProgrammingLanguage{ID: "lang-1", Code: "golang", Name: "Golang", Version: "1.24"})
	repo.mustStore(t, &model.ProgrammingLanguage{ID: "lang-2", Code: "golang", Name: "Golang", Version: "1.25"})

	version := "1.24"
	_, err := svc.Update(context.Background(), "lang-2", UpdateProgrammingLanguageCommand{Version: &version})

	require.ErrorIs(t, err, bcode.ErrProgrammingLanguageExists)
}

func TestProgrammingLanguageServiceUpdateMapsRepositoryConflict(t *testing.T) {
	repo := newFakeProgrammingLanguageRepo()
	svc := &programmingLanguageServiceImpl{LanguageRepo: repo}
	repo.mustStore(t, &model.ProgrammingLanguage{
		ID:      "lang-1",
		Code:    "golang",
		Name:    "Golang",
		Version: "1.24",
		Enabled: boolPtr(true),
	})
	repo.updateErr = datastore.ErrRecordExist

	name := "Go"
	_, err := svc.Update(context.Background(), "lang-1", UpdateProgrammingLanguageCommand{Name: &name})

	require.ErrorIs(t, err, bcode.ErrProgrammingLanguageExists)
}

func TestProgrammingLanguageServiceRejectsInvalidResources(t *testing.T) {
	svc := &programmingLanguageServiceImpl{LanguageRepo: newFakeProgrammingLanguageRepo()}
	enabled := true

	_, err := svc.Create(context.Background(), CreateProgrammingLanguageCommand{
		Name:    "Golang",
		Version: "1.24",
		Enabled: &enabled,
		CPUReq:  "bad-cpu",
	})

	require.ErrorIs(t, err, bcode.ErrProgrammingLanguageInvalid)
}

func TestProgrammingLanguageServiceRejectsTooLongGeneratedCode(t *testing.T) {
	svc := &programmingLanguageServiceImpl{LanguageRepo: newFakeProgrammingLanguageRepo()}
	enabled := true

	_, err := svc.Create(context.Background(), CreateProgrammingLanguageCommand{
		Name:    strings.Repeat("+", 33),
		Version: "1.24",
		Enabled: &enabled,
	})

	require.ErrorIs(t, err, bcode.ErrProgrammingLanguageInvalid)
}

type fakeProgrammingLanguageRepo struct {
	items      map[string]*model.ProgrammingLanguage
	updateErr  error
	updateTime time.Time
}

func newFakeProgrammingLanguageRepo() *fakeProgrammingLanguageRepo {
	return &fakeProgrammingLanguageRepo{items: map[string]*model.ProgrammingLanguage{}}
}

func (f *fakeProgrammingLanguageRepo) FindByID(_ context.Context, id string) (*model.ProgrammingLanguage, error) {
	item, ok := f.items[id]
	if !ok {
		return nil, datastore.ErrRecordNotExist
	}
	return cloneProgrammingLanguage(item), nil
}

func (f *fakeProgrammingLanguageRepo) FindByCodeVersion(_ context.Context, code, version string) (*model.ProgrammingLanguage, error) {
	for _, item := range f.items {
		if item.Code == code && item.Version == version {
			return cloneProgrammingLanguage(item), nil
		}
	}
	return nil, datastore.ErrRecordNotExist
}

func (f *fakeProgrammingLanguageRepo) Create(_ context.Context, language *model.ProgrammingLanguage) error {
	if _, exists := f.items[language.ID]; exists {
		return datastore.ErrRecordExist
	}
	if _, err := f.FindByCodeVersion(context.Background(), language.Code, language.Version); err == nil {
		return datastore.ErrRecordExist
	}
	f.items[language.ID] = cloneProgrammingLanguage(language)
	return nil
}

func (f *fakeProgrammingLanguageRepo) Update(_ context.Context, language *model.ProgrammingLanguage) error {
	if _, exists := f.items[language.ID]; !exists {
		return datastore.ErrRecordNotExist
	}
	if f.updateErr != nil {
		return f.updateErr
	}
	for _, item := range f.items {
		if item.ID != language.ID && item.Code == language.Code && item.Version == language.Version {
			return datastore.ErrRecordExist
		}
	}
	if !f.updateTime.IsZero() {
		language.UpdateTime = f.updateTime
	}
	f.items[language.ID] = cloneProgrammingLanguage(language)
	return nil
}

func (f *fakeProgrammingLanguageRepo) Delete(_ context.Context, language *model.ProgrammingLanguage) error {
	if _, exists := f.items[language.ID]; !exists {
		return datastore.ErrRecordNotExist
	}
	delete(f.items, language.ID)
	return nil
}

func (f *fakeProgrammingLanguageRepo) List(context.Context, datastore.ListOptions) ([]*model.ProgrammingLanguage, error) {
	out := make([]*model.ProgrammingLanguage, 0, len(f.items))
	for _, item := range f.items {
		out = append(out, cloneProgrammingLanguage(item))
	}
	return out, nil
}

func (f *fakeProgrammingLanguageRepo) ListByQuery(context.Context, *model.ProgrammingLanguage, datastore.ListOptions) ([]*model.ProgrammingLanguage, error) {
	return nil, nil
}

func (f *fakeProgrammingLanguageRepo) mustStore(t *testing.T, language *model.ProgrammingLanguage) {
	t.Helper()
	require.NoError(t, f.Create(context.Background(), language))
}

func cloneProgrammingLanguage(in *model.ProgrammingLanguage) *model.ProgrammingLanguage {
	if in == nil {
		return nil
	}
	out := *in
	if in.Enabled != nil {
		enabled := *in.Enabled
		out.Enabled = &enabled
	}
	return &out
}

func boolPtr(value bool) *bool {
	return &value
}

package programminglanguage

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/internal/traitvalidation"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

var (
	languageCodeRegexp    = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)
	languageVersionRegexp = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]*$`)
)

// CreateProgrammingLanguageCommand contains transport-independent create input.
type CreateProgrammingLanguageCommand struct {
	Name    string
	Version string
	Enabled *bool
	CPUReq  string
	MemReq  string
}

// UpdateProgrammingLanguageCommand contains transport-independent mutable fields.
type UpdateProgrammingLanguageCommand struct {
	Name    *string
	Version *string
	Enabled *bool
	CPUReq  *string
	MemReq  *string
}

// ProgrammingLanguageService manages administrator-configured programming languages.
type ProgrammingLanguageService interface {
	List(ctx context.Context) ([]*model.ProgrammingLanguage, error)
	Get(ctx context.Context, id string) (*model.ProgrammingLanguage, error)
	Create(ctx context.Context, command CreateProgrammingLanguageCommand) (*model.ProgrammingLanguage, error)
	Update(ctx context.Context, id string, command UpdateProgrammingLanguageCommand) (*model.ProgrammingLanguage, error)
	Delete(ctx context.Context, id string) error
}

type programmingLanguageServiceImpl struct {
	LanguageRepo repository.ProgrammingLanguageRepository `inject:""`
}

// NewProgrammingLanguageService creates a ProgrammingLanguageService.
func NewProgrammingLanguageService() ProgrammingLanguageService {
	return &programmingLanguageServiceImpl{}
}

// NewProgrammingLanguageServiceWithRepository creates a ready-to-use service.
func NewProgrammingLanguageServiceWithRepository(repo repository.ProgrammingLanguageRepository) (ProgrammingLanguageService, error) {
	if repo == nil {
		return nil, fmt.Errorf("create programming language service: repository is nil")
	}
	return &programmingLanguageServiceImpl{LanguageRepo: repo}, nil
}

func (s *programmingLanguageServiceImpl) List(ctx context.Context) ([]*model.ProgrammingLanguage, error) {
	items, err := s.LanguageRepo.List(ctx, datastore.ListOptions{
		Page:     0,
		PageSize: 0,
		SortBy: []datastore.SortOption{
			{Key: "code", Order: datastore.SortOrderAscending},
			{Key: "version", Order: datastore.SortOrderAscending},
		},
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

func (s *programmingLanguageServiceImpl) Get(ctx context.Context, id string) (*model.ProgrammingLanguage, error) {
	language, err := s.LanguageRepo.FindByID(ctx, strings.TrimSpace(id))
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrProgrammingLanguageNotFound
		}
		return nil, err
	}
	return language, nil
}

func (s *programmingLanguageServiceImpl) Create(ctx context.Context, command CreateProgrammingLanguageCommand) (*model.ProgrammingLanguage, error) {
	if command.Enabled == nil {
		return nil, bcode.ErrProgrammingLanguageInvalid
	}
	name := strings.TrimSpace(command.Name)
	code := generateProgrammingLanguageCode(name)
	version := strings.TrimSpace(command.Version)
	cpuReq := strings.TrimSpace(command.CPUReq)
	memReq := strings.TrimSpace(command.MemReq)
	if err := validateProgrammingLanguageFields(code, name, version, cpuReq, memReq); err != nil {
		return nil, err
	}
	if _, err := s.LanguageRepo.FindByCodeVersion(ctx, code, version); err == nil {
		return nil, bcode.ErrProgrammingLanguageExists
	} else if !errors.Is(err, datastore.ErrRecordNotExist) {
		return nil, err
	}

	language := &model.ProgrammingLanguage{
		ID:      utils.RandStringByNumLowercase(24),
		Code:    code,
		Name:    name,
		Version: version,
		Enabled: command.Enabled,
		CPUReq:  cpuReq,
		MemReq:  memReq,
	}
	if err := s.LanguageRepo.Create(ctx, language); err != nil {
		if errors.Is(err, datastore.ErrRecordExist) {
			return nil, bcode.ErrProgrammingLanguageExists
		}
		return nil, err
	}
	return language, nil
}

func (s *programmingLanguageServiceImpl) Update(ctx context.Context, id string, command UpdateProgrammingLanguageCommand) (*model.ProgrammingLanguage, error) {
	language, err := s.LanguageRepo.FindByID(ctx, strings.TrimSpace(id))
	if err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, bcode.ErrProgrammingLanguageNotFound
		}
		return nil, err
	}
	if !hasProgrammingLanguageUpdate(command) {
		return nil, bcode.ErrProgrammingLanguageInvalid
	}

	code := strings.TrimSpace(language.Code)
	name := strings.TrimSpace(language.Name)
	version := strings.TrimSpace(language.Version)
	cpuReq := strings.TrimSpace(language.CPUReq)
	memReq := strings.TrimSpace(language.MemReq)
	if command.Name != nil {
		name = strings.TrimSpace(*command.Name)
	}
	if command.Version != nil {
		version = strings.TrimSpace(*command.Version)
	}
	if command.CPUReq != nil {
		cpuReq = strings.TrimSpace(*command.CPUReq)
	}
	if command.MemReq != nil {
		memReq = strings.TrimSpace(*command.MemReq)
	}
	if err := validateProgrammingLanguageFields(code, name, version, cpuReq, memReq); err != nil {
		return nil, err
	}
	if version != language.Version {
		existing, err := s.LanguageRepo.FindByCodeVersion(ctx, code, version)
		if err == nil && existing != nil && existing.ID != language.ID {
			return nil, bcode.ErrProgrammingLanguageExists
		}
		if err != nil && !errors.Is(err, datastore.ErrRecordNotExist) {
			return nil, err
		}
	}

	language.Name = name
	language.Version = version
	language.CPUReq = cpuReq
	language.MemReq = memReq
	if command.Enabled != nil {
		language.Enabled = command.Enabled
	}
	if err := s.LanguageRepo.Update(ctx, language); err != nil {
		if errors.Is(err, datastore.ErrRecordExist) {
			return nil, bcode.ErrProgrammingLanguageExists
		}
		return nil, err
	}
	return language, nil
}

func (s *programmingLanguageServiceImpl) Delete(ctx context.Context, id string) error {
	language := &model.ProgrammingLanguage{ID: strings.TrimSpace(id)}
	if err := s.LanguageRepo.Delete(ctx, language); err != nil {
		if errors.Is(err, datastore.ErrRecordNotExist) {
			return bcode.ErrProgrammingLanguageNotFound
		}
		return err
	}
	return nil
}

func generateProgrammingLanguageCode(name string) string {
	var segments []string
	var current strings.Builder
	flushCurrent := func() {
		if current.Len() == 0 {
			return
		}
		segments = append(segments, current.String())
		current.Reset()
	}

	for _, r := range strings.TrimSpace(name) {
		switch {
		case r >= 'A' && r <= 'Z':
			current.WriteRune(r + ('a' - 'A'))
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			current.WriteRune(r)
		case isProgrammingLanguageCodeSeparator(r):
			flushCurrent()
		default:
			flushCurrent()
			segments = append(segments, fmt.Sprintf("%x", r))
		}
	}
	flushCurrent()

	return strings.Join(segments, "-")
}

func isProgrammingLanguageCodeSeparator(r rune) bool {
	return r == '.' || r == '_' || r == '-' || unicode.IsSpace(r)
}

func validateProgrammingLanguageFields(code, name, version, cpuReq, memReq string) error {
	if code == "" || len(code) > 64 || !languageCodeRegexp.MatchString(code) {
		return bcode.ErrProgrammingLanguageInvalid
	}
	if name == "" || len(name) > 128 {
		return bcode.ErrProgrammingLanguageInvalid
	}
	if version == "" || len(version) > 64 || !languageVersionRegexp.MatchString(version) {
		return bcode.ErrProgrammingLanguageInvalid
	}
	errors := traitvalidation.ValidateResourcesTraitSpec(spec.ResourceTraitsSpec{
		CPU:    cpuReq,
		Memory: memReq,
	}, "programmingLanguage")
	if len(errors) > 0 {
		return fmt.Errorf("%w: %s", bcode.ErrProgrammingLanguageInvalid, errors[0].Message)
	}
	return nil
}

func hasProgrammingLanguageUpdate(command UpdateProgrammingLanguageCommand) bool {
	return command.Name != nil ||
		command.Version != nil ||
		command.Enabled != nil ||
		command.CPUReq != nil ||
		command.MemReq != nil
}

package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/repository"
	apiauth "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/auth"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/cache"
)

type oauth struct {
	SettingRepo repository.SystemSettingRepository `inject:""`
	Cache       cache.ICache                       `inject:"cache"`
}

// NewOAuth creates OAuth API handler.
func NewOAuth() Interface {
	return &oauth{}
}

func (o *oauth) RegisterRoutes(group *gin.RouterGroup) {
	// Temporary disable OAuth routes on master; uncomment when OAuth login should be enabled again.
	// group.GET("/auth/oauth2/google/login", o.googleLogin)
	// group.GET("/auth/oauth2/google/callback", o.googleCallback)
}

func (o *oauth) googleLogin(c *gin.Context) {
	flow := apiauth.NewOAuthGoogleLoginFlow(o.SettingRepo, o.Cache, nil)
	redirectURL, err := flow.BuildLoginURL(c.Request.Context())
	if err != nil {
		returnOAuthError(c, err)
		return
	}
	c.Redirect(http.StatusFound, redirectURL)
}

func (o *oauth) googleCallback(c *gin.Context) {
	code := strings.TrimSpace(c.Query("code"))
	state := strings.TrimSpace(c.Query("state"))

	flow := apiauth.NewOAuthGoogleLoginFlow(o.SettingRepo, o.Cache, nil)
	result, err := flow.HandleCallback(c.Request.Context(), code, state)
	if err != nil {
		returnOAuthError(c, err)
		return
	}

	// OAuth callback returns bearer token payload; mark response as non-cacheable.
	c.Header("Cache-Control", "no-store")
	c.Header("Pragma", "no-cache")
	c.Header("Expires", "0")

	bcode.ReturnSuccess(c, apis.OAuthLoginResponse{
		AccessToken: result.AccessToken,
		TokenType:   result.TokenType,
		ExpiresIn:   result.ExpiresIn,
		Subject:     result.Subject,
		Email:       result.Email,
		Roles:       result.Roles,
	})
}

func returnOAuthError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, apiauth.ErrOAuthSettingNotFound), errors.Is(err, apiauth.ErrOAuthConfigInvalid):
		bcode.ReturnError(c, bcode.ErrOAuthConfigInvalid)
	case errors.Is(err, apiauth.ErrOAuthStateInvalid):
		bcode.ReturnError(c, bcode.ErrOAuthStateInvalid)
	case errors.Is(err, apiauth.ErrOAuthCodeExchangeFailed):
		bcode.ReturnError(c, bcode.ErrOAuthCodeExchangeFailed)
	case errors.Is(err, apiauth.ErrOAuthUserInfoFailed):
		bcode.ReturnError(c, bcode.ErrOAuthUserInfoFetchFailed)
	case errors.Is(err, apiauth.ErrOAuthRoleMappingFailed):
		bcode.ReturnError(c, bcode.ErrOAuthRoleMappingFailed)
	case errors.Is(err, apiauth.ErrOAuthTokenIssueFailed):
		bcode.ReturnError(c, bcode.ErrOAuthTokenIssueFailed)
	default:
		bcode.ReturnError(c, err)
	}
}

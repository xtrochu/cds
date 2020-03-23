package api

import (
	"context"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/ovh/cds/engine/api/application"
	"github.com/ovh/cds/engine/api/project"
	"github.com/ovh/cds/engine/service"
	"github.com/ovh/cds/sdk"
	"github.com/ovh/cds/sdk/exportentities"
)

func (api *API) getApplicationExportHandler() service.Handler {
	return func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		// Get project name in URL
		vars := mux.Vars(r)
		projectKey := vars[permProjectKey]
		appName := vars["applicationName"]

		format := FormString(r, "format")
		if format == "" {
			format = "yaml"
		}

		// Export
		f, err := exportentities.GetFormat(format)
		if err != nil {
			return sdk.WrapError(err, "Format invalid")
		}

		proj, err := project.Load(api.mustDB(), api.Cache, projectKey)
		if err != nil {
			return sdk.WrapError(err, "cannot load project %s", projectKey)
		}

		if _, err := application.Export(ctx, api.mustDB(), proj.ID, appName, f, project.EncryptWithBuiltinKey, w); err != nil {
			return sdk.WrapError(err, "getApplicationExportHandler")
		}

		w.Header().Add("Content-Type", exportentities.GetContentType(f))
		w.WriteHeader(http.StatusOK)

		return nil
	}
}

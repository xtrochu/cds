package api

import (
	"context"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/ovh/cds/engine/api/application"
	"github.com/ovh/cds/engine/api/metrics"
	"github.com/ovh/cds/engine/api/project"
	"github.com/ovh/cds/engine/service"
	"github.com/ovh/cds/sdk"
)

func (api *API) getApplicationMetricHandler() service.Handler {
	return func(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
		vars := mux.Vars(r)
		projectKey := vars[permProjectKey]
		appName := vars["applicationName"]
		metricName := vars["metricName"]

		proj, err := project.Load(api.mustDB(), api.Cache, projectKey)
		if err != nil {
			return sdk.WrapError(err, "cannot load project %s", projectKey)
		}

		app, err := application.LoadByProjectIDAndName(ctx, api.mustDB(), proj.ID, appName)
		if err != nil {
			return err
		}

		result, err := metrics.GetMetrics(ctx, api.mustDB(), proj.Key, app.ID, metricName)
		if err != nil {
			return sdk.WrapError(err, "cannot get metrics")
		}

		return service.WriteJSON(w, result, http.StatusOK)
	}
}

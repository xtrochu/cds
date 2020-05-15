package application

import (
	"context"

	"github.com/go-gorp/gorp"

	"github.com/ovh/cds/sdk"
)

/*
// LoadOptionFunc is a type for all options in LoadOptions
type LoadOptionFunc *func(gorp.SqlExecutor, *sdk.Application) error

// LoadOptions provides all options on project loads functions
var LoadOptions = struct {
	Default                        LoadOptionFunc
	WithVariables                  LoadOptionFunc
	WithVariablesWithClearPassword LoadOptionFunc
	WithKeys                       LoadOptionFunc
	WithClearKeys                  LoadOptionFunc
	WithDeploymentStrategies       LoadOptionFunc
	WithClearDeploymentStrategies  LoadOptionFunc
	WithVulnerabilities            LoadOptionFunc
	WithIcon                       LoadOptionFunc
}{
	Default:                        &loadDefaultDependencies,
	WithVariables:                  &loadVariables,
	WithVariablesWithClearPassword: &loadVariablesWithClearPassword,
	WithKeys:                       &loadKeys,
	WithClearKeys:                  &loadClearKeys,
	WithDeploymentStrategies:       &loadDeploymentStrategies,
	WithClearDeploymentStrategies:  &loadDeploymentStrategiesWithClearPassword,
	WithVulnerabilities:            &loadVulnerabilities,
  WithIcon:                       &loadIcon,



var (
	loadDefaultDependencies = func(db gorp.SqlExecutor, app *sdk.Application) error {
		if err := loadVariables(db, app); err != nil && sdk.Cause(err) != sql.ErrNoRows {
			return sdk.WrapError(err, "application.loadDefaultDependencies %s", app.Name)
		}
		return nil
	}

	loadVariables = func(db gorp.SqlExecutor, app *sdk.Application) error {
		variables, err := LoadAllVariables(db, app.ID)
		if err != nil && sdk.Cause(err) != sql.ErrNoRows {
			return sdk.WrapError(err, "Unable to load variables for application %d", app.ID)
		}
		app.Variables = variables
		return nil
	}

	loadVariablesWithClearPassword = func(db gorp.SqlExecutor, app *sdk.Application) error {
		variables, err := LoadAllVariablesWithDecrytion(db, app.ID)
		if err != nil && sdk.Cause(err) != sql.ErrNoRows {
			return sdk.WrapError(err, "Unable to load variables for application %d", app.ID)
		}
		app.Variables = variables
		return nil
	}

	loadKeys = func(db gorp.SqlExecutor, app *sdk.Application) error {
		keys, err := LoadAllKeys(db, app.ID)
		if err != nil {
			return err
		}
		app.Keys = keys
		return nil
	}

	loadClearKeys = func(db gorp.SqlExecutor, app *sdk.Application) error {
		keys, err := LoadAllKeysWithPrivateContent(db, app.ID)
		if err != nil {
			return err
		}
		app.Keys = keys
		return nil
	}

	loadDeploymentStrategies = func(db gorp.SqlExecutor, app *sdk.Application) error {
		var err error
		app.DeploymentStrategies, err = LoadDeploymentStrategies(db, app.ID, false)
		if err != nil && sdk.Cause(err) != sql.ErrNoRows {
			return sdk.WrapError(err, "Unable to load deployment strategies for application %d", app.ID)
		}
		return nil
	}

	loadIcon = func(db gorp.SqlExecutor, app *sdk.Application) error {
		var err error
		app.Icon, err = LoadIcon(db, app.ID)
		if err != nil && sdk.Cause(err) != sql.ErrNoRows {
			return sdk.WrapError(err, "Unable to load icon")
		}
		return nil
	}

	loadVulnerabilities = func(db gorp.SqlExecutor, app *sdk.Application) error {
		var err error
		app.Vulnerabilities, err = LoadVulnerabilities(db, app.ID)
		if err != nil && sdk.Cause(err) != sql.ErrNoRows {
			return sdk.WrapError(err, "Unable to load vulnerabilities")
		}
		return nil
	}

	loadDeploymentStrategiesWithClearPassword = func(db gorp.SqlExecutor, app *sdk.Application) error {
		var err error
		app.DeploymentStrategies, err = LoadDeploymentStrategies(db, app.ID, true)
		if err != nil && sdk.Cause(err) != sql.ErrNoRows {
			return sdk.WrapError(err, "Unable to load deployment strategies for application %d", app.ID)
		}
		return nil
	}
)
*/

// LoadOptionFunc is a type for all options in LoadOptions
type LoadOptionFunc func(context.Context, gorp.SqlExecutor, ...*sdk.Application) error

// LoadOptions provides all options on project loads functions
var LoadOptions = struct {
	Default                        LoadOptionFunc
	WithVariables                  LoadOptionFunc
	WithVariablesWithClearPassword LoadOptionFunc
	WithKeys                       LoadOptionFunc
	WithClearKeys                  LoadOptionFunc
	WithDeploymentStrategies       LoadOptionFunc
	WithClearDeploymentStrategies  LoadOptionFunc
	WithVulnerabilities            LoadOptionFunc
	WithIcon                       LoadOptionFunc
}{
	Default:                        loadDefault,
	WithVariables:                  loadVariables,
	WithVariablesWithClearPassword: loadVariablesWithClearPassword,
	WithKeys:                       loadKeys,
	WithClearKeys:                  loadClearKeys,
	WithDeploymentStrategies:       loadDeploymentStrategies,
	WithClearDeploymentStrategies:  loadDeploymentStrategiesWithClearPassword,
	WithVulnerabilities:            loadVulnerabilities,
	WithIcon:                       loadIcon,
}

func loadDefault(ctx context.Context, db gorp.SqlExecutor, as ...*sdk.Application) error {
	if err := loadVariables(ctx, db, as...); err != nil {
		return err
	}
	return nil
}

func loadVariables(ctx context.Context, db gorp.SqlExecutor, as ...*sdk.Application) error {
	for i := range as {
		variables, err := LoadVariables(ctx, db, as[i].ID)
		if err != nil {
			return sdk.WrapError(err, "unable to load variables for application %d", as[i].ID)
		}
		as[i].Variables = variables
	}
	return nil
}

func loadVariablesWithClearPassword(ctx context.Context, db gorp.SqlExecutor, as ...*sdk.Application) error {
	for i := range as {
		variables, err := LoadVariablesWithDecrytion(ctx, db, as[i].ID)
		if err != nil {
			return sdk.WrapError(err, "unable to load variables for application %d", as[i].ID)
		}
		as[i].Variables = variables
	}
	return nil
}

func loadKeys(ctx context.Context, db gorp.SqlExecutor, as ...*sdk.Application) error {
	for i := range as {
		keys, err := LoadAllKeys(ctx, db, as[i].ID)
		if err != nil {
			return err
		}
		as[i].Keys = keys
	}
	return nil
}

func loadClearKeys(ctx context.Context, db gorp.SqlExecutor, as ...*sdk.Application) error {
	for i := range as {
		keys, err := LoadKeysWithPrivateContent(ctx, db, as[i].ID)
		if err != nil {
			return err
		}
		as[i].Keys = keys
	}
	return nil
}

func loadDeploymentStrategies(ctx context.Context, db gorp.SqlExecutor, as ...*sdk.Application) error {
	for i := range as {
		deploymentStrategies, err := LoadDeploymentStrategies(db, as[i].ID, false)
		if err != nil {
			return sdk.WrapError(err, "Unable to load deployment strategies for application %d", as[i].ID)
		}
		as[i].DeploymentStrategies = deploymentStrategies
	}
	return nil
}

func loadIcon(ctx context.Context, db gorp.SqlExecutor, as ...*sdk.Application) error {
	for i := range as {
		icon, err := LoadIcon(db, as[i].ID)
		if err != nil {
			return sdk.WrapError(err, "unable to load icon")
		}
		as[i].Icon = icon
	}
	return nil
}

func loadVulnerabilities(ctx context.Context, db gorp.SqlExecutor, as ...*sdk.Application) error {
	for i := range as {
		vulnerabilities, err := LoadVulnerabilities(db, as[i].ID)
		if err != nil {
			return sdk.WrapError(err, "unable to load vulnerabilities")
		}
		as[i].Vulnerabilities = vulnerabilities
	}
	return nil
}

func loadDeploymentStrategiesWithClearPassword(ctx context.Context, db gorp.SqlExecutor, as ...*sdk.Application) error {
	for i := range as {
		deploymentStrategies, err := LoadDeploymentStrategies(db, as[i].ID, true)
		if err != nil {
			return sdk.WrapError(err, "unable to load deployment strategies for application %d", as[i].ID)
		}
		as[i].DeploymentStrategies = deploymentStrategies
	}
	return nil
}

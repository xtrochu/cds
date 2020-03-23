package application

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-gorp/gorp"
	"github.com/lib/pq"

	"github.com/ovh/cds/engine/api/cache"
	"github.com/ovh/cds/engine/api/database/gorpmapping"
	"github.com/ovh/cds/sdk"
)

const appRows = `
application.id,
application.name,
application.project_id,
application.repo_fullname,
application.repositories_manager_id,
application.last_modified,
application.metadata,
application.vcs_server,
application.vcs_strategy,
application.description,
application.from_repository
`

func getAll(ctx context.Context, db gorp.SqlExecutor, opts []LoadOptionFunc, query string, args ...interface{}) ([]sdk.Application, error) {
	var res []dbApplication
	if _, err := db.Select(&res, query, args...); err != nil {
		if err == sql.ErrNoRows {
			return nil, sdk.WrapError(sdk.ErrApplicationNotFound, "application.loadapplications")
		}
		return nil, sdk.WrapError(err, "application.loadapplications")
	}

	apps := make([]sdk.Application, len(res))
	for i := range res {
		a := &res[i]
		if err := a.PostGet(db); err != nil {
			return nil, sdk.WrapError(err, "application.loadapplications")
		}
		app, err := unwrap(ctx, db, opts, a)
		if err != nil {
			return nil, sdk.WrapError(err, "application.loadapplications")
		}
		apps[i] = *app
	}

	return apps, nil
}

func get(ctx context.Context, db gorp.SqlExecutor, opts []LoadOptionFunc, query string, args ...interface{}) (*sdk.Application, error) {
	dbApp := dbApplication{}
	if err := db.SelectOne(&dbApp, query, args...); err != nil {
		if err == sql.ErrNoRows {
			return nil, sdk.WithStack(sdk.ErrApplicationNotFound)
		}
		return nil, sdk.WrapError(err, "application.load")
	}
	return unwrap(ctx, db, opts, &dbApp)
}

func unwrap(ctx context.Context, db gorp.SqlExecutor, opts []LoadOptionFunc, dbApp *dbApplication) (*sdk.Application, error) {
	app := sdk.Application(*dbApp)
	for _, f := range opts {
		if err := f(ctx, db, &app); err != nil {
			return nil, err
		}
	}
	return &app, nil
}

// Exists checks if an application given its name exists
func Exists(db gorp.SqlExecutor, projectKey, appName string) (bool, error) {
	count, err := db.SelectInt("SELECT count(1) FROM application join project ON project.id = application.project_id WHERE project.projectkey = $1 AND application.name = $2", projectKey, appName)
	if err != nil {
		return false, err
	}
	return count == 1, nil
}

// LoadAllByProjectIDAndRepository load all application where repository match given one.
func LoadAllByProjectIDAndRepository(ctx context.Context, db gorp.SqlExecutor, projectID int64, repo string) ([]sdk.Application, error) {
	query := fmt.Sprintf(`
    SELECT %s
    FROM application
    WHERE project_id = $1
    AND from_repository = $2
  `, appRows)
	args := []interface{}{projectID, repo}
	return getAll(ctx, db, nil, query, args...)
}

// LoadByProjectIDAndName load an application from DB.
func LoadByProjectIDAndName(ctx context.Context, db gorp.SqlExecutor, projectID int64, name string, opts ...LoadOptionFunc) (*sdk.Application, error) {
	query := fmt.Sprintf(`
    SELECT %s
    FROM application
    WHERE project_id = $1
    AND name = $2
  `, appRows)
	args := []interface{}{projectID, name}
	return get(ctx, db, opts, query, args...)
}

// LoadByID load an application from DB.
func LoadByID(ctx context.Context, db gorp.SqlExecutor, id int64, opts ...LoadOptionFunc) (*sdk.Application, error) {
	query := fmt.Sprintf(`
    SELECT %s
    FROM application
    WHERE application.id = $1
  `, appRows)
	return get(ctx, db, opts, query, id)
}

// LoadByWorkflowID loads applications from database for a given workflow id
func LoadByWorkflowID(db gorp.SqlExecutor, workflowID int64) ([]sdk.Application, error) {
	apps := []sdk.Application{}
	query := fmt.Sprintf(`
    SELECT DISTINCT %s
	  FROM application
		JOIN w_node_context ON w_node_context.application_id = application.id
		JOIN w_node ON w_node.id = w_node_context.node_id
		JOIN workflow ON workflow.id = w_node.workflow_id
    WHERE workflow.id = $1
  `, appRows)
	if _, err := db.Select(&apps, query, workflowID); err != nil {
		if err == sql.ErrNoRows {
			return apps, nil
		}
		return nil, sdk.WrapError(err, "Unable to load applications linked to workflow id %d", workflowID)
	}
	return apps, nil
}

// Insert add an application id database
func Insert(db gorp.SqlExecutor, store cache.Store, projectID int64, app *sdk.Application) error {
	if err := app.IsValid(); err != nil {
		return sdk.WrapError(err, "application is not valid")
	}

	app.ProjectID = projectID
	app.LastModified = time.Now()

	dbApp := dbApplication(*app)
	if err := db.Insert(&dbApp); err != nil {
		if errPG, ok := err.(*pq.Error); ok && errPG.Code == gorpmapping.ViolateUniqueKeyPGCode {
			err = sdk.ErrApplicationExist
		}
		return sdk.WrapError(err, "application.Insert %s(%d)", app.Name, app.ID)
	}
	*app = sdk.Application(dbApp)

	return nil
}

// Update updates application id database
func Update(db gorp.SqlExecutor, store cache.Store, app *sdk.Application) error {
	if err := app.IsValid(); err != nil {
		return sdk.WrapError(err, "application is not valid")
	}

	app.LastModified = time.Now()
	dbApp := dbApplication(*app)
	n, err := db.Update(&dbApp)
	if err != nil {
		return sdk.WrapError(err, "application.Update %s(%d)", app.Name, app.ID)
	}
	if n == 0 {
		return sdk.WrapError(sdk.ErrApplicationNotFound, "application.Update %s(%d)", app.Name, app.ID)
	}

	return nil
}

// LoadAll returns all applications.
func LoadAll(ctx context.Context, db gorp.SqlExecutor, projectID int64, opts ...LoadOptionFunc) ([]sdk.Application, error) {
	query := fmt.Sprintf(`
		SELECT %s
		FROM application
		WHERE project_id = $1
		ORDER BY name ASC`, appRows)
	return getAll(ctx, db, opts, query, projectID)
}

// LoadAllNames returns all application names
func LoadAllNames(db gorp.SqlExecutor, projectID int64) (sdk.IDNames, error) {
	query := `
		SELECT application.id, application.name, application.description, application.icon
		FROM application
		WHERE application.project_id= $1
		ORDER BY application.name ASC`

	var res sdk.IDNames
	if _, err := db.Select(&res, query, projectID); err != nil {
		if err == sql.ErrNoRows {
			return res, nil
		}
		return nil, sdk.WrapError(err, "application.loadapplicationnames")
	}

	return res, nil
}

// LoadIcon return application icon given his application id.
func LoadIcon(db gorp.SqlExecutor, appID int64) (string, error) {
	icon, err := db.SelectStr("SELECT icon FROM application WHERE id = $1", appID)
	return icon, sdk.WithStack(err)
}

package application

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-gorp/gorp"

	"github.com/ovh/cds/engine/api/database/gorpmapping"
	"github.com/ovh/cds/sdk"
	"github.com/ovh/cds/sdk/log"
)

func getVariables(ctx context.Context, db gorp.SqlExecutor, query gorpmapping.Query, opts ...gorpmapping.GetOptionFunc) ([]sdk.Variable, error) {
	var res []dbApplicationVariable
	vars := make([]sdk.Variable, 0, len(res))

	if err := gorpmapping.GetAll(ctx, db, query, &res, opts...); err != nil {
		return nil, err
	}

	for i := range res {
		isValid, err := gorpmapping.CheckSignature(res[i], res[i].Signature)
		if err != nil {
			return nil, err
		}
		if !isValid {
			log.Error(ctx, "application.getAllVariables> application key %d data corrupted", res[i].ID)
			continue
		}
		vars = append(vars, res[i].Variable())
	}
	return vars, nil
}

func getVariable(ctx context.Context, db gorp.SqlExecutor, q gorpmapping.Query, opts ...gorpmapping.GetOptionFunc) (*sdk.Variable, error) {
	var v dbApplicationVariable
	found, err := gorpmapping.Get(ctx, db, q, &v, opts...)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, sdk.WithStack(sdk.ErrNotFound)
	}
	isValid, err := gorpmapping.CheckSignature(v, v.Signature)
	if err != nil {
		return nil, err
	}
	if !isValid {
		log.Error(ctx, "application.loadVariable> application variable %d data corrupted", v.ID)
		return nil, sdk.WithStack(sdk.ErrNotFound)
	}

	res := v.Variable()
	return &res, err
}

// LoadVariables for the given application.
func LoadVariables(ctx context.Context, db gorp.SqlExecutor, appID int64) ([]sdk.Variable, error) {
	query := gorpmapping.NewQuery(`
		SELECT *
		FROM application_variable
		WHERE application_id = $1
		ORDER BY var_name
	`).Args(appID)
	return getVariables(ctx, db, query)
}

// LoadVariablesWithDecrytion for the given application, it also decrypt all the secure content.
func LoadVariablesWithDecrytion(ctx context.Context, db gorp.SqlExecutor, appID int64) ([]sdk.Variable, error) {
	query := gorpmapping.NewQuery(`
		SELECT *
		FROM application_variable
		WHERE application_id = $1
		ORDER BY var_name
	`).Args(appID)
	return getVariables(ctx, db, query, gorpmapping.GetOptions.WithDecryption)
}

// LoadVariable retrieve a specific variable
func LoadVariable(ctx context.Context, db gorp.SqlExecutor, appID int64, varName string) (*sdk.Variable, error) {
	query := gorpmapping.NewQuery(`SELECT * FROM application_variable
			WHERE application_id = $1 AND var_name=$2`).Args(appID, varName)
	return getVariable(ctx, db, query)
}

// LoadVariableWithDecryption retrieve a specific variable with decrypted content
func LoadVariableWithDecryption(ctx context.Context, db gorp.SqlExecutor, appID int64, varID int64, varName string) (*sdk.Variable, error) {
	query := gorpmapping.NewQuery(`SELECT * FROM application_variable
			WHERE application_id = $1 AND id = $2 AND var_name=$3`).Args(appID, varID, varName)
	return getVariable(ctx, db, query, gorpmapping.GetOptions.WithDecryption)
}

// InsertVariable inserts a new variable in the given application.
func InsertVariable(db gorp.SqlExecutor, appID int64, v *sdk.Variable, u sdk.Identifiable) error {
	//Check variable name
	rx := sdk.NamePatternRegex
	if !rx.MatchString(v.Name) {
		return sdk.NewError(sdk.ErrInvalidName, fmt.Errorf("Invalid variable name. It should match %s", sdk.NamePattern))
	}

	dbVar := newDBApplicationVariable(*v, appID)
	err := gorpmapping.InsertAndSign(context.Background(), db, &dbVar)
	if err != nil && strings.Contains(err.Error(), "application_variable_pkey") {
		return sdk.WithStack(sdk.ErrVariableExists)

	}
	if err != nil {
		return sdk.WrapError(err, "Cannot insert variable %s", v.Name)
	}

	*v = dbVar.Variable()

	ava := &sdk.ApplicationVariableAudit{
		ApplicationID: appID,
		Type:          sdk.AuditAdd,
		Author:        u.GetUsername(),
		VariableAfter: *v,
		VariableID:    v.ID,
		Versionned:    time.Now(),
	}

	if err := inserAudit(db, ava); err != nil {
		return sdk.WrapError(err, "Cannot insert audit for variable %d", v.ID)
	}
	return nil
}

// UpdateVariable updates a variable in the given application.
func UpdateVariable(db gorp.SqlExecutor, appID int64, variable *sdk.Variable, variableBefore *sdk.Variable, u sdk.Identifiable) error {
	rx := sdk.NamePatternRegex
	if !rx.MatchString(variable.Name) {
		return sdk.NewError(sdk.ErrInvalidName, fmt.Errorf("Invalid variable name. It should match %s", sdk.NamePattern))
	}

	dbVar := newDBApplicationVariable(*variable, appID)

	if err := gorpmapping.UpdateAndSign(context.Background(), db, &dbVar); err != nil {
		return err
	}

	*variable = dbVar.Variable()

	if variableBefore == nil && u == nil {
		return nil
	}

	ava := &sdk.ApplicationVariableAudit{
		ApplicationID:  appID,
		Type:           sdk.AuditUpdate,
		Author:         u.GetUsername(),
		VariableAfter:  *variable,
		VariableBefore: variableBefore,
		VariableID:     variable.ID,
		Versionned:     time.Now(),
	}

	if err := inserAudit(db, ava); err != nil {
		return sdk.WrapError(err, "Cannot insert audit for variable %s", variable.Name)
	}

	return nil
}

// DeleteVariable removes a variable from the given application.
func DeleteVariable(db gorp.SqlExecutor, appID int64, variable *sdk.Variable, u sdk.Identifiable) error {
	query := `DELETE FROM application_variable
		  WHERE application_variable.application_id = $1 AND application_variable.var_name = $2`
	result, err := db.Exec(query, appID, variable.Name)
	if err != nil {
		return sdk.WrapError(err, "Cannot delete variable %s", variable.Name)
	}

	rowAffected, err := result.RowsAffected()
	if err != nil {
		return sdk.WithStack(err)
	}
	if rowAffected == 0 {
		return sdk.WithStack(ErrNoVariable)
	}

	ava := &sdk.ApplicationVariableAudit{
		ApplicationID:  appID,
		Type:           sdk.AuditDelete,
		Author:         u.GetUsername(),
		VariableBefore: variable,
		VariableID:     variable.ID,
		Versionned:     time.Now(),
	}

	if err := inserAudit(db, ava); err != nil {
		return sdk.WrapError(err, "Cannot insert audit for variable %s", variable.Name)
	}
	return nil
}

// DeleteVariablesByApplicationID removes all variables for given application.
func DeleteVariablesByApplicationID(db gorp.SqlExecutor, applicationID int64) error {
	query := `DELETE FROM application_variable
	          WHERE application_id = $1`
	if _, err := db.Exec(query, applicationID); err != nil {
		return sdk.WithStack(err)
	}
	return nil
}

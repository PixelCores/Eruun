package sql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"k8s.io/klog/v2"

	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/datastore"
)

// Driver is a unified implementation of SQL driver of datastore
type Driver struct {
	Client gorm.DB
}

func normalizeWriteError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return datastore.ErrRecordExist
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return datastore.ErrRecordNotExist
	}
	return datastore.NewDBError(err)
}

// Add a data model
func (m *Driver) Add(ctx context.Context, entity datastore.Entity) error {
	if entity.PrimaryKey() == "" {
		return datastore.ErrPrimaryEmpty
	}
	if entity.TableName() == "" {
		return datastore.ErrTableNameEmpty
	}
	entity.SetCreateTime(time.Now())
	entity.SetUpdateTime(time.Now())

	if dbAdd := m.Client.WithContext(ctx).Create(entity); dbAdd.Error != nil {
		return normalizeWriteError(dbAdd.Error)
	}
	return nil
}

// BatchAdd batch adds entities atomically in a single transaction.
func (m *Driver) BatchAdd(ctx context.Context, entities []datastore.Entity) error {
	return batchAddWithTransaction(ctx, m, entities)
}

type batchAddTxRunner interface {
	WithTransaction(ctx context.Context, fn func(tx datastore.DataStore) error) error
}

func batchAddWithTransaction(ctx context.Context, txRunner batchAddTxRunner, entities []datastore.Entity) error {
	if len(entities) == 0 {
		return nil
	}
	err := txRunner.WithTransaction(ctx, func(tx datastore.DataStore) error {
		for _, saveEntity := range entities {
			if err := tx.Add(ctx, saveEntity); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		return nil
	}
	var dbErr *datastore.DBError
	if errors.As(err, &dbErr) {
		return err
	}
	return datastore.NewDBError(fmt.Errorf("batch add entities: %w", err))
}

// Get get data model
func (m *Driver) Get(ctx context.Context, entity datastore.Entity) error {
	if entity.PrimaryKey() == "" {
		return datastore.ErrPrimaryEmpty
	}
	if entity.TableName() == "" {
		return datastore.ErrTableNameEmpty
	}

	if dbGet := m.Client.WithContext(ctx).First(entity); dbGet.Error != nil {
		if errors.Is(dbGet.Error, gorm.ErrRecordNotFound) {
			return datastore.ErrRecordNotExist
		}
		return datastore.NewDBError(dbGet.Error)
	}
	return nil
}

// Put update data model
func (m *Driver) Put(ctx context.Context, entity datastore.Entity) error {
	if entity.PrimaryKey() == "" {
		return datastore.ErrPrimaryEmpty
	}
	if entity.TableName() == "" {
		return datastore.ErrTableNameEmpty
	}
	entity.SetUpdateTime(time.Now())
	if dbPut := m.Client.WithContext(ctx).Model(entity).Updates(entity); dbPut.Error != nil {
		return normalizeWriteError(dbPut.Error)
	}
	return nil
}

// IsExist determine whether data exists.
func (m *Driver) IsExist(ctx context.Context, entity datastore.Entity) (bool, error) {
	if entity == nil {
		return false, datastore.ErrNilEntity
	}
	table := entity.TableName()
	if table == "" {
		return false, datastore.ErrTableNameEmpty
	}

	conds := entity.Index()
	if len(conds) == 0 {
		return false, fmt.Errorf("IsExist: no query condition provided")
	}

	dest, err := datastore.NewEntity(entity)
	if err != nil {
		return false, err
	}
	tx := m.Client.WithContext(ctx).Table(table).Where(conds).Limit(1).Find(dest)
	if tx.Error != nil {
		if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, datastore.NewDBError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return false, nil
	}
	return true, nil
}

func (m *Driver) IsExistByCondition(ctx context.Context, table string, cond map[string]interface{}, dest interface{}) (bool, error) {
	if table == "" {
		return false, datastore.ErrTableNameEmpty
	}
	if len(cond) == 0 {
		return false, errors.New("condition map is empty")
	}

	tx := m.Client.WithContext(ctx).Table(table).Where(cond).Limit(1).Find(dest)
	if tx.Error != nil {
		if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, datastore.NewDBError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		return false, nil
	}
	return true, nil
}

// Delete delete data
func (m *Driver) Delete(ctx context.Context, entity datastore.Entity) error {
	if entity.PrimaryKey() == "" {
		return datastore.ErrPrimaryEmpty
	}
	if entity.TableName() == "" {
		return datastore.ErrTableNameEmpty
	}
	// check entity is existed
	if err := m.Get(ctx, entity); err != nil {
		return err
	}

	if dbDelete := m.Client.WithContext(ctx).Model(entity).Delete(entity); dbDelete.Error != nil {
		klog.Errorf("delete document failure %v", dbDelete.Error)
		return datastore.NewDBError(dbDelete.Error)
	}

	return nil
}

// DeleteByFilter deletes entities matching the provided index fields and filter options.
func (m *Driver) DeleteByFilter(ctx context.Context, entity datastore.Entity, options *datastore.FilterOptions) error {
	if entity == nil {
		return datastore.ErrNilEntity
	}
	table := entity.TableName()
	if table == "" {
		return datastore.ErrTableNameEmpty
	}

	var exprs []clause.Expression
	for k, v := range entity.Index() {
		exprs = append(exprs, clause.Eq{
			Column: strings.ToLower(k),
			Value:  v,
		})
	}
	if options != nil {
		exprs = _applyFilterOptions(exprs, *options)
	}
	if len(exprs) == 0 {
		return datastore.NewDBError(fmt.Errorf("delete by filter requires at least one condition"))
	}

	clauses := []clause.Expression{clause.Where{Exprs: exprs}}
	target, err := datastore.NewEntity(entity)
	if err != nil {
		return err
	}
	if dbDelete := m.Client.WithContext(ctx).Table(table).Clauses(clauses...).Delete(target); dbDelete.Error != nil {
		klog.Errorf("delete by filter failure %v", dbDelete.Error)
		return datastore.NewDBError(dbDelete.Error)
	}
	return nil
}

// _toColumnName converts keys of the models to lowercase as the column name are in lowercase in the database
func _toColumnName(columnName string) string {
	return strings.ToLower(columnName)
}

func _applyFilterOptions(clauses []clause.Expression, filterOptions datastore.FilterOptions) []clause.Expression {
	for _, queryOp := range filterOptions.Queries {
		clauses = append(clauses, clause.Like{
			Column: _toColumnName(queryOp.Key),
			Value:  fmt.Sprintf("%%%s%%", queryOp.Query),
		})
	}
	for _, queryOp := range filterOptions.In {
		values := make([]interface{}, len(queryOp.Values))
		for i, v := range queryOp.Values {
			values[i] = v
		}
		clauses = append(clauses, clause.IN{
			Column: _toColumnName(queryOp.Key),
			Values: values,
		})
	}
	for _, queryOp := range filterOptions.NotEqual {
		clauses = append(clauses, clause.Neq{
			Column: _toColumnName(queryOp.Key),
			Value:  queryOp.Value,
		})
	}
	for _, queryOp := range filterOptions.LessThan {
		clauses = append(clauses, clause.Lt{
			Column: _toColumnName(queryOp.Key),
			Value:  queryOp.Value,
		})
	}
	for _, queryOp := range filterOptions.IsNotExist {
		clauses = append(clauses, clause.Eq{
			Column: _toColumnName(queryOp.Key),
			Value:  "",
		})
	}
	return clauses
}

// List entity function
func (m *Driver) List(ctx context.Context, entity datastore.Entity, op *datastore.ListOptions) ([]datastore.Entity, error) {
	if entity == nil {
		return nil, datastore.ErrNilEntity
	}
	table := entity.TableName()
	if table == "" {
		return nil, datastore.ErrTableNameEmpty
	}

	var (
		exprs   []clause.Expression
		clauses []clause.Expression
	)

	// 加入 entity.Index() 条件
	for k, v := range entity.Index() {
		exprs = append(exprs, clause.Eq{
			Column: strings.ToLower(k),
			Value:  v,
		})
	}

	// 应用外部传入的 FilterOptions
	if op != nil {
		exprs = _applyFilterOptions(exprs, op.FilterOptions)
	}

	if len(exprs) > 0 {
		clauses = append(clauses, clause.Where{Exprs: exprs})
	}

	// 分页处理
	if op != nil && op.PageSize > 0 && op.Page > 0 {
		limit := op.PageSize
		offset := op.PageSize * (op.Page - 1)
		clauses = append(clauses, clause.Limit{Limit: &limit, Offset: offset})
	}

	// 排序处理
	if op != nil && len(op.SortBy) > 0 {
		var orderBy []clause.OrderByColumn
		for _, sort := range op.SortBy {
			orderBy = append(orderBy, clause.OrderByColumn{
				Column: clause.Column{Name: strings.ToLower(sort.Key)},
				Desc:   sort.Order == datastore.SortOrderDescending,
			})
		}
		clauses = append(clauses, clause.OrderBy{Columns: orderBy})
	}

	// 查询执行
	var list []datastore.Entity
	rows, err := m.Client.WithContext(ctx).Table(table).Clauses(clauses...).Rows()
	if err != nil {
		return nil, datastore.NewDBError(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			klog.Warningf("close rows failure %s", err.Error())
		}
	}()

	for rows.Next() {
		item, err := datastore.NewEntity(entity)
		if err != nil {
			return nil, datastore.NewDBError(err)
		}
		if err := m.Client.WithContext(ctx).ScanRows(rows, item); err != nil {
			return nil, datastore.NewDBError(fmt.Errorf("row scan failure %w", err))
		}
		list = append(list, item)
	}

	if err := rows.Err(); err != nil {
		return nil, datastore.NewDBError(err)
	}

	return list, nil
}

// Count counts entities
func (m *Driver) Count(ctx context.Context, entity datastore.Entity, filterOptions *datastore.FilterOptions) (int64, error) {
	if entity.TableName() == "" {
		return 0, datastore.ErrTableNameEmpty
	}
	var (
		count   int64
		exprs   []clause.Expression
		clauses []clause.Expression
	)
	for k, v := range entity.Index() {
		exprs = append(exprs, clause.Eq{
			Column: strings.ToLower(k),
			Value:  v,
		})
	}
	if filterOptions != nil {
		exprs = _applyFilterOptions(exprs, *filterOptions)
	}
	if len(exprs) > 0 {
		clauses = append(clauses, clause.Where{
			Exprs: exprs,
		})
	}
	if dbCount := m.Client.WithContext(ctx).Model(entity).Clauses(clauses...).Count(&count); dbCount.Error != nil {
		return 0, datastore.NewDBError(dbCount.Error)
	}
	return count, nil
}

// CompareAndSwap performs an atomic compare-and-swap update.
// It updates the entity only if the current value of conditionField matches conditionValue.
// Returns (true, nil) if update succeeded, (false, nil) if condition not met.
func (m *Driver) CompareAndSwap(ctx context.Context, entity datastore.Entity, conditionField string, conditionValue interface{}, updates map[string]interface{}) (bool, error) {
	return m.CompareAndSwapWithConditions(ctx, entity, map[string]interface{}{
		conditionField: conditionValue,
	}, updates)
}

// CompareAndSwapWithConditions performs an atomic compare-and-swap update with multiple conditions.
// It updates the entity only if all conditions match.
// Returns (true, nil) if update succeeded, (false, nil) if condition not met.
func (m *Driver) CompareAndSwapWithConditions(ctx context.Context, entity datastore.Entity, conditions map[string]interface{}, updates map[string]interface{}) (bool, error) {
	if entity.PrimaryKey() == "" {
		return false, datastore.ErrPrimaryEmpty
	}
	if entity.TableName() == "" {
		return false, datastore.ErrTableNameEmpty
	}
	if len(conditions) == 0 {
		return false, datastore.NewDBError(fmt.Errorf("compare and swap requires at least one condition"))
	}

	fields := make(map[string]interface{}, len(updates)+1)
	for k, v := range updates {
		fields[k] = v
	}
	now := time.Now()
	// Add update_time to updates (using lowercase as per SQLNamer convention)
	fields["update_time"] = now

	// Build the query with primary key and all conditions.
	query := applyCompareAndSwapConditions(m.Client.WithContext(ctx).Model(entity), conditions)
	result := query.Updates(fields)

	if result.Error != nil {
		return false, normalizeWriteError(result.Error)
	}

	// RowsAffected > 0 means the condition matched and update succeeded
	return syncCompareAndSwapUpdateTime(entity, result.RowsAffected > 0, now), nil
}

func applyCompareAndSwapConditions(query *gorm.DB, conditions map[string]interface{}) *gorm.DB {
	for field, value := range conditions {
		if value == nil {
			query = query.Where(_toColumnName(field) + " IS NULL")
			continue
		}
		query = query.Where(_toColumnName(field)+" = ?", value)
	}
	return query
}

func syncCompareAndSwapUpdateTime(entity datastore.Entity, updated bool, updateTime time.Time) bool {
	if updated {
		entity.SetUpdateTime(updateTime)
	}
	return updated
}

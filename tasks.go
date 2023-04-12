package tasks

import (
	"context"
	"fmt"
	"ngs-core/components/laboratory/models/common"
	"ngs-core/components/laboratory/models/journal"
	"ngs-core/components/laboratory/models/states"
	"ngs-core/components/laboratory/models/task"
	"ngs-core/components/laboratory/tubes/operations"
	gmodel "ngs-core/graph/model"
	"ngs-core/libs/base"
	"ngs-core/libs/db"
	"ngs-core/libs/gqlext"
	"ngs-core/libs/log"
	"ngs-core/pmodel/entities"
	"ngs-core/pmodel/repos"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
)

var (
	labLock = &sync.RWMutex{}
)

func HasConflicts(ctx context.Context, tx pgx.Tx, task *task.LabTask) (bool, error) {
	sql := fmt.Sprintf(`
SELECT EXISTS(SELECT m.task_id FROM "evogen"."lab_process_tasks_samples" m
LEFT JOIN "evogen"."lab_process_tasks" t ON t.id = m.task_id
WHERE m.lab_id IN %s AND t.state = $1 AND step = $2 AND m.task_id <> $3)`,
		db.AsIDTuple(task.GetLabIDs()))
	rows, err := tx.Query(ctx, sql, states.InProgressID, task.Step, task.ID)
	if err != nil {
		log.Errorf("Failed to check lab task conflicts %v", err)
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		exists := false
		err = rows.Scan(&exists)
		if err != nil {
			log.Errorf("Failed to check lab task conflicts %v", err)
			return false, err
		}
		return exists, nil
	}
	return false, nil
}

func SaveTask(ctx context.Context, task *task.LabTask) (int64, error) {
	if task == nil {
		return 0, nil
	}
	if len(task.Step) == 0 {
		return 0, fmt.Errorf("step is missing the lab task")
	}
	labLock.Lock()
	defer labLock.Unlock()
	tx, err := db.DB.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if ok, err := HasConflicts(ctx, tx, task); ok || err != nil {
		return 0, fmt.Errorf("this task conflicts with another task with the same lab ids %v", err)
	}
	task.AddDetails()
	key := base.GenerateUUID()
	task.Details["_key"] = key
	_, err = tx.Exec(ctx, `INSERT INTO "evogen"."lab_process_tasks"(performer, created_at, created_by,  details, state, step, state_ts) 
		VALUES ($1, $6, $2, $3,$4, $5, $6)`,
		task.Performer, task.CreatedBy, task.Details, task.State, task.Step, time.Now().UTC())
	if err != nil {
		return 0, err
	}
	rows, err := tx.Query(ctx, `SELECT id FROM "evogen"."lab_process_tasks" WHERE details ->> '_key' = $1`, key)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var id int64
	for rows.Next() {
		err = rows.Scan(&id)
		if err != nil {
			log.Errorf("Failed to read task id %v", err)
			return 0, err
		}
		break
	}
	rows.Close()
	if err != nil {
		log.Errorf("Failed to read created task id %v", err)
		return 0, err
	}
	task.ID = id
	for _, library := range task.Samples {
		_, err := tx.Exec(ctx, `
INSERT INTO "evogen"."lab_process_tasks_samples" (task_id, lab_id, created_at, created_by, details)
VALUES($1, $2, $5, $3, $4)`, id, library.SampleID, library.CreatedBy, library.Details, time.Now().UTC())
		if err != nil {
			log.Errorf("Failed to add sample to the task %v", err)
			return 0, err
		}
	}
	prevStep := task.GetPreviousStep()
	if prevStep != nil {
		_, err := tx.Exec(ctx, `
UPDATE
	"evogen"."lab_process_tasks_samples" as m
SET
parent_id = (SELECT ts.task_id FROM "evogen"."lab_process_tasks_samples" ts
	LEFT JOIN "evogen"."lab_process_tasks" t ON t.id = ts.task_id
	WHERE 
	t.step = $1 AND 
	t.state = $2 AND
	m.lab_id = ts.lab_id
	ORDER BY ts.task_id DESC LIMIT 1)
WHERE 
	m.task_id = $3
`, *prevStep, states.DoneID, id)
		if err != nil {
			log.Errorf("Failed to update parent state %v", err)
			return 0, err
		}
	}
	err = tx.Commit(ctx)
	if err != nil {
		log.Errorf("Failed to save task %v", err)
	}
	return id, err
}

func RemoveSamples(ctx context.Context, taskID int64) error {
	labLock.Lock()
	defer labLock.Unlock()

	tx, err := db.DB.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `DELETE FROM "evogen"."lab_process_tasks_samples" WHERE task_id = $1`, taskID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func UpdateTask(ctx context.Context, task *task.LabTask) error {
	labLock.Lock()
	defer labLock.Unlock()
	tx, err := db.DB.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if ok, err := HasConflicts(ctx, tx, task); ok || err != nil {
		return fmt.Errorf("this task conflicts with another task with the same lab ids %v", err)
	}
	_, err = tx.Exec(ctx, `
UPDATE "evogen"."lab_process_tasks"
SET
	performer = $2, updated_at = $7, updated_by = $3,  details = $4, state = $5, step = $6
WHERE id = $1`,
		task.ID, task.Performer, task.UpdatedBy, task.Details, task.State, task.Step, time.Now().UTC())
	if err != nil {
		return err
	}
	for _, it := range task.Samples {
		library := it
		if library.ID < 0 {
			_, err := tx.Exec(ctx, `INSERT INTO "evogen"."lab_process_tasks_samples" (task_id, lab_id, created_at, created_by, details)
VALUES($1, $2, $5, $3, $4)
`, library.TaskID, library.SampleID, library.CreatedBy, library.Details, time.Now().UTC())
			if err != nil {
				return err
			}
		} else {
			_, err := tx.Exec(ctx, `UPDATE "evogen"."lab_process_tasks_samples" 
SET  updated_at = $5, updated_by = $1, details = $2, lab_id = $3 WHERE ID = $4
`, library.UpdatedBy, library.Details, library.SampleID, library.ID, time.Now().UTC())
			if err != nil {
				return err
			}
		}
	}
	prevStep := task.GetPreviousStep()
	if prevStep != nil {
		_, err := tx.Exec(ctx, `
UPDATE
	"evogen"."lab_process_tasks_samples" as m
SET
parent_id = (SELECT ts.task_id FROM "evogen"."lab_process_tasks_samples" ts
	LEFT JOIN "evogen"."lab_process_tasks" t ON t.id = ts.task_id
	WHERE 
	t.step = $1 AND 
	t.state = $2 AND
	m.lab_id = ts.lab_id
	ORDER BY ts.task_id DESC LIMIT 1)
WHERE 
	m.task_id = $3 AND
	m.parent_id is NULL
`, *prevStep, states.DoneID, task.ID)
		if err != nil {
			log.Errorf("Failed to update parent state %v", err)
			return err
		}
	}
	return tx.Commit(ctx)
}

func RemoveTask(ctx context.Context, id int64) error {
	labLock.Lock()
	defer labLock.Unlock()

	tx, err := db.DB.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `DELETE FROM "evogen"."evogen"."lab_process_tasks_samples" WHERE task_id = $1`, id)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `DELETE FROM "evogen"."lab_process_tasks" WHERE id = $1`, id)
	err = operations.RevertChanges(ctx, tx, id)
	if err != nil {
		log.Errorf("Failed to revert tubes changes %v", err)
		return err
	}
	return tx.Commit(ctx)
}

func statusToID(vec []string) []string {
	result := make([]string, len(vec))
	for index := range vec {
		st := gmodel.ELabOperationStatus(vec[index])
		status := journal.G2PLabOperationStatus(&st)
		id := states.EntryStatusToState(status)
		result[index] = id
	}
	return result
}

func buildTaskQuery(ctx context.Context, steps []string, skipSort bool) string {
	r := &repos.Repository{}
	sql := `LEFT JOIN "evogen"."staff" AS staff ON staff.id = m.performer
			LEFT JOIN "evogen"."states" AS states ON states.id = m.state
			WHERE ( m.step in ` + db.AsIDTuple(steps) + ` ) `
	filterRule := ""
	sortRule := ""
	filter, ok := ctx.Value(base.CtxKeyFilter).(entities.Filter)
	if ok && len(filter.Text) > 0 {
		escaped := db.FuzzyMatchValue(filter.Text)
		filterRule = fmt.Sprintf(`
			(
			m.created_by ILIKE %[1]s OR
			m.updated_by ILIKE %[1]s OR
			staff.first_name 	ILIKE %[1]s OR
			staff.last_name 	ILIKE %[1]s OR
			staff.middle_name 	ILIKE %[1]s OR
			staff.login	ILIKE %[1]s OR
			staff.email			ILIKE %[1]s OR
			states.name 		ILIKE %[1]s OR
			m.step 				ILIKE %[1]s OR
			m.details ->> 'library_number' ILIKE %[1]s OR
			EXISTS (
				select distinct lib.lab_id from "evogen"."lab_process_tasks_samples" lib 
				where lib.task_id = m.id and lib.lab_id ILIKE %[1]s
			)) `, escaped)
	}
	if ok && len(filter.Fields) > 0 {
		if len(filterRule) > 0 {
			filterRule += ` AND `
		}
		filterRule += ` (`
		first := true
		for k, v := range filter.Fields {
			if len(v) == 0 {
				continue
			}
			if first {
				first = false
			} else {
				filterRule += " AND "
			}
			switch k {
			case "birthday":
				fallthrough
			case "createdAt":
				fallthrough
			case "updatedAt":
				field := r.MapCommonField(k)
				if len(v) != 2 {
					log.Errorf("Expected two values for date/time field %v, %v", k, v)
				}
				sql, err := db.FilterByDate("m", field, v)
				if err != nil {
					log.Errorf("Failed to generate filter rule %v", err)
					continue
				}
				filterRule += sql
			case "step":
				filterRule += db.STupleFilter(`m`, `step`, v)
			case "id":
				filterRule += db.TupleFilter(`m`, `id`, v)
			case "status":
				fallthrough
			case "state":
				filterRule += db.STupleFilter(`states`, `id`, statusToID(v))
			case "performer":
				filterRule += db.STupleFilter(`m`, `performer`, v)
			case "libraryNumber":
				filterRule += db.STupleFilter(`m`, `details ->> 'library_number'`, v)
			case "labId", "lab_id":
				filterRule += ` EXISTS (
					select distinct lib.lab_id from "evogen"."lab_process_tasks_samples" lib 
					where lib.task_id = m.id and lib.lab_id in ` + db.AsIDTuple(v) + `) `
			default:
				// skip
			}
		}
		filterRule += ` ) `
	}
	if !skipSort {
		sort, ok := ctx.Value(base.CtxKeySort).(entities.Sort)
		if ok && !sort.IsNoSort() {
			switch sort.Field {
			case "fullName":
				sortRule = fmt.Sprintf(`ORDER BY concat(staff.first_name,  staff.middle_name,  staff.last_name) %s `,
					sort.ToDir())
			case "email":
				sortRule = fmt.Sprintf(`ORDER BY staff.email %s`, sort.ToDir())
			case "login":
				sortRule = fmt.Sprintf(`ORDER BY staff.login %s`, sort.ToDir())
			case "step":
				sortRule = fmt.Sprintf(`ORDER BY m.step %s`, sort.ToDir())
			case "state":
				fallthrough
			case "status":
				sortRule = fmt.Sprintf(`ORDER BY m.state %s`, sort.ToDir())
			case "samples":
				sortRule = fmt.Sprintf(`ORDER BY samplesCount %s`, sort.ToDir())
			case "priority":
				//NOTE: overwrite on priority from request
				//sortRule = fmt.Sprintf(`ORDER BY m.details ->> 'priority' %s`, sort.ToDir())
				sortRule = fmt.Sprintf(`ORDER BY high_priority %s`, sort.ToDir())
			case "libraryNumber":
				sortRule = fmt.Sprintf(`ORDER BY m.details ->> 'library_number' %s`, sort.ToDir())
			default:
				sortRule = fmt.Sprintf(`ORDER BY %s %s`, r.MapCommonField(sort.Field), sort.ToDir())
			}
		}
	}
	if len(filterRule) > 0 {
		sql += ` AND ` + filterRule
	}
	if len(sortRule) > 0 {
		sql += sortRule
	} else if !skipSort {
		sql += ` ORDER BY m.created_at DESC`
	}
	if !skipSort {
		sql += ` LIMIT $1 OFFSET $2`
	}
	return sql
}

func LoadTask(ctx context.Context, id int64) (*task.LabTask, error) {
	labLock.RLock()
	defer labLock.RUnlock()

	s, ctx, err := db.GetOrCreateConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer s.Release()
	rows, err := s.Conn.Query(ctx,
		`SELECT m.id, m.created_at, m.created_by, m.updated_at, m.updated_by, m.details, m.performer, m.state,
			m.step,
			m.state_ts,
			(select array(select distinct lib.id from "evogen"."lab_process_tasks_samples" lib where lib.task_id = m.id ))
			FROM "evogen"."lab_process_tasks" as m WHERE id = $1`, id)
	if err != nil {
		log.Errorf("Failed to query for client %v", err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		task := task.LabTask{}
		err = rows.Scan(
			&task.ID,
			&task.CreatedAt,
			&task.CreatedBy,
			&task.UpdatedAt,
			&task.UpdatedBy,
			&task.Details,
			&task.Performer,
			&task.State,
			&task.Step,
			&task.StateTS,
			&task.SampleIDs)
		if err != nil {
			return nil, err
		}
		return &task, nil
	}
	return nil, db.ErrRecordNotFound
}

func LoadTaskWithSamples(ctx context.Context, id int64) (*task.LabTask, error) {
	labLock.RLock()
	defer labLock.RUnlock()

	s, ctx, err := db.GetOrCreateConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer s.Release()
	rows, err := s.Conn.Query(ctx,
		`SELECT 
m.id, m.created_at, m.created_by, m.updated_at, m.updated_by, m.details, m.performer, m.state, m.step, m.state_ts
FROM "evogen"."lab_process_tasks" as m WHERE id = $1`, id)
	if err != nil {
		log.Errorf("Failed to query for client %v", err)
		return nil, err
	}
	var result *task.LabTask
	for rows.Next() {
		task := task.LabTask{
			Samples: make([]task.TaskSample, 0, 16),
		}
		err = rows.Scan(
			&task.ID,
			&task.CreatedAt,
			&task.CreatedBy,
			&task.UpdatedAt,
			&task.UpdatedBy,
			&task.Details,
			&task.Performer,
			&task.State,
			&task.Step,
			&task.StateTS)
		if err != nil {
			rows.Close()
			return nil, err
		}
		result = &task
		break
	}
	rows.Close()
	if result == nil {
		return nil, db.ErrRecordNotFound
	}
	rows, err = s.Conn.Query(ctx, `
SELECT m.id, m.created_at, m.created_by, m.updated_at, m.updated_by, m.details, m.lab_id FROM "evogen"."lab_process_tasks_samples" m 
WHERE m.task_id = $1 ORDER BY m.id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		lib := task.TaskSample{}
		err = rows.Scan(
			&lib.ID,
			&lib.CreatedAt,
			&lib.CreatedBy,
			&lib.UpdatedAt,
			&lib.UpdatedBy,
			&lib.Details,
			&lib.SampleID)
		if err != nil {
			return nil, err
		}
		result.Samples = append(result.Samples, lib)
	}
	return result, nil
}

func convertSteps(s []gmodel.ELabStep) []string {
	if s == nil {
		return []string{
			gmodel.ELabStepVcf.String(),
			gmodel.ELabStepLibrary.String(),
			gmodel.ELabStepSequencerRun.String(),
		}
	}
	v := make([]string, len(s))
	for index := range s {
		v[index] = s[index].String()
	}
	return v
}

func QuerySamplesWithState(ctx context.Context, steps []gmodel.ELabStep, states []string) (map[common.SampleID]struct{}, error) {
	labLock.RLock()
	defer labLock.RUnlock()

	var stepValues []interface{}
	for _, step := range steps {
		stepValues = append(stepValues, step.String())
	}
	args := make([]interface{}, 0, len(steps)+len(states))
	stepParameters, args := db.AsParametrizedValuesSet([][]interface{}{stepValues}, 1)

	var statusValues []interface{}
	for _, status := range states {
		statusValues = append(statusValues, status)
	}
	statusParameters, statusArgs := db.AsParametrizedValuesSet([][]interface{}{statusValues}, len(args)+1)
	args = append(args, statusArgs...)

	s, ctx, err := db.GetOrCreateConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer s.Release()
	sql := `SELECT  DISTINCT lab_id FROM  "evogen"."lab_process_tasks_samples" m
LEFT JOIN "evogen"."lab_process_tasks" t on t.id = m.task_id 
WHERE t.step in ` + stepParameters + ` AND t.state in ` + statusParameters
	rows, err := s.Conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := map[common.SampleID]struct{}{}
	for rows.Next() {
		id := ""
		err = rows.Scan(&id)
		if err != nil {
			return nil, err
		}
		m[common.SampleID(id)] = struct{}{}
	}
	return m, nil
}

func QuerySamplesWithStateForSequencerRunParams(ctx context.Context, steps []gmodel.ELabStep, states []string) (map[common.SampleID]struct{}, error) {
	labLock.RLock()
	defer labLock.RUnlock()

	args := make([]interface{}, 0, len(steps)+len(states))
	
	var statusValues []interface{}
	for _, status := range states {
		statusValues = append(statusValues, status)
	}
	statusParameters, statusArgs := db.AsParametrizedValuesSet([][]interface{}{statusValues}, len(args)+1)
	args = append(args, statusArgs...)

	s, ctx, err := db.GetOrCreateConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer s.Release()

	sql := `SELECT  DISTINCT lab_id FROM  "evogen"."lab_process_sruns" s
	WHERE state in ` + statusParameters

	rows, err := s.Conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := map[common.SampleID]struct{}{}
	for rows.Next() {
		id := ""
		err = rows.Scan(&id)
		if err != nil {
			return nil, err
		}
		m[common.SampleID(id)] = struct{}{}
	}
	return m, nil
}

func QuerySamplesWithLabTask(ctx context.Context, steps []gmodel.ELabStep) (map[common.SampleID]struct{}, error) {
	labLock.RLock()
	defer labLock.RUnlock()

	stepsArr := make([]interface{}, 0, len(steps))
	for _, step := range steps {
		stepsArr = append(stepsArr, step.String())
	}
	valuesSet, arguments := db.AsParametrizedValuesSet([][]interface{}{stepsArr}, 1)

	s, ctx, err := db.GetOrCreateConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer s.Release()
	sql := `SELECT  DISTINCT lab_id FROM  "evogen"."lab_process_tasks_samples" m
LEFT JOIN "evogen"."lab_process_tasks" t on t.id = m.task_id 
WHERE t.step in ` + valuesSet
	rows, err := s.Conn.Query(ctx, sql, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := map[common.SampleID]struct{}{}
	for rows.Next() {
		id := ""
		err = rows.Scan(&id)
		if err != nil {
			return nil, err
		}
		m[common.SampleID(id)] = struct{}{}
	}
	return m, nil
}

func QueryAsMap(ctx context.Context, _steps []gmodel.ELabStep) (map[common.SampleID][]task.LabTask, error) {
	s, ctx, err := db.GetOrCreateConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer s.Release()
	vec, _, err := Query(ctx, _steps, -1, 0)
	if err != nil {
		return nil, err
	}
	samples, err := QueryLabTaskSamples(ctx)
	if err != nil {
		return nil, err
	}
	result := map[common.SampleID][]task.LabTask{}
	for index := range vec {
		for _, id := range vec[index].SampleIDs {
			sample, ok := samples[id]
			if !ok {
				continue
			}
			vec[index].Samples = append(vec[index].Samples, sample)
		}
	}
	for _, it := range vec {
		for _, sample := range it.Samples {
			tmp, ok := result[sample.SampleID]
			if !ok {
				tmp = make([]task.LabTask, 0, 1)
			}
			tmp = append(tmp, it)
			result[sample.SampleID] = tmp
		}
	}
	return result, nil
}

func QueryLabTaskSamples(ctx context.Context) (map[int64]task.TaskSample, error) {
	labLock.RLock()
	defer labLock.RUnlock()

	s, ctx, err := db.GetOrCreateConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer s.Release()
	rows, err := s.Conn.Query(ctx, `SELECT id, task_id, lab_id, details, parent_id FROM "evogen"."lab_process_tasks_samples"`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[int64]task.TaskSample)
	for rows.Next() {
		taskSample := task.TaskSample{}
		err = rows.Scan(&taskSample.ID, &taskSample.TaskID, &taskSample.SampleID, &taskSample.Details, &taskSample.ParentID)
		if err != nil {
			return nil, err
		}
		result[taskSample.ID] = taskSample
	}
	return result, nil
}

func Query(ctx context.Context, _steps []gmodel.ELabStep, limit, offset int) ([]task.LabTask, int, error) {
	labLock.RLock()
	defer labLock.RUnlock()

	steps := convertSteps(_steps)
	s, ctx, err := db.GetOrCreateConnection(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer s.Release()
	countSQL := `SELECT count(m.id) FROM "evogen"."lab_process_tasks" as m ` + buildTaskQuery(ctx, steps, true)
	totalCount, err := countWithArgs(ctx, countSQL)
	if err != nil {
		log.Errorf("Cannot compute total count of clients %v", err)
		return nil, 0, err
	}

	limit, offset = fixLimitAndOffset(ctx, limit, offset)
	sql := `SELECT 
	m.id, m.created_at, m.created_by, m.updated_at, m.updated_by, m.details, m.performer, m.state, m.step, m.state_ts,
		(select array(select distinct lib.id from "evogen"."lab_process_tasks_samples" lib where lib.task_id = m.id )),
		(select array(select distinct lib.lab_id from "evogen"."lab_process_tasks_samples" lib where lib.task_id = m.id )),
		(select count (distinct lib.id) from "evogen"."lab_process_tasks_samples" lib where lib.task_id = m.id ) samplesCount,
        (SELECT EXISTS(SELECT r.id FROM "evogen"."requests" r
                                INNER JOIN "evogen"."lab_process_tasks_samples" lpts
                                           ON r.lab_id = lpts.lab_id AND r.is_deleted = FALSE
                       WHERE lpts.task_id = M.id AND r.high_priority = TRUE)) high_priority
			FROM "evogen"."lab_process_tasks" as m ` + buildTaskQuery(ctx, steps, false)
	rows, err := s.Conn.Query(ctx, sql, limit, offset)
	if err != nil {
		log.Errorf("Failed to query for client %v", err)
		return nil, 0, err
	}
	defer rows.Close()
	vec := make([]task.LabTask, 0, 100)
	for rows.Next() {
		task := task.LabTask{}
		var highPriority bool
		err = rows.Scan(
			&task.ID,
			&task.CreatedAt,
			&task.CreatedBy,
			&task.UpdatedAt,
			&task.UpdatedBy,
			&task.Details,
			&task.Performer,
			&task.State,
			&task.Step,
			&task.StateTS,
			&task.SampleIDs,
			&task.LabIDs,
			nil,
			&highPriority)
		if err != nil {
			log.Errorf("Failed to parse lab task %v", err)
			return nil, 0, err
		}
		if task.Details == nil {
			task.Details = map[string]interface{}{}
		}
		task.Details["request-priority"] = highPriority
		vec = append(vec, task)
	}
	return vec, totalCount, nil
}

func RemoveLibraryFromLabTask(ctx context.Context, taskID int64, libraryID int64) error {
	labLock.Lock()
	defer labLock.Unlock()

	tx, err := db.DB.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	cmd, err := tx.Exec(ctx, `DELETE FROM "evogen"."lab_process_tasks_samples" WHERE task_id = $1 and id = $2`,
		taskID, libraryID)
	if !cmd.Delete() {
		return db.ErrRecordNotFound
	}
	return tx.Commit(ctx)
}

func ChangeStatus(ctx context.Context, taskID int64, status gmodel.ELabOperationStatus) error {
	labLock.Lock()
	defer labLock.Unlock()

	user := gqlext.User(ctx)
	tmp := task.LabTask{}
	tmp.SetStatus(status)
	tx, err := db.DB.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT count(id) FROM "evogen"."lab_process_tasks_samples" WHERE task_id = $1`, taskID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		count := 0
		err = rows.Scan(&count)
		if err != nil {
			return err
		}
		if count == 0 {
			return fmt.Errorf("cannot change status of empty task")
		}
	}
	rows.Close()
	cmd, err := tx.Exec(ctx, `UPDATE "evogen"."lab_process_tasks"
SET 
	state = $1,
	updated_at = $4,
	updated_by = $2,
	state_ts = $4
WHERE id = $3`,
		tmp.State, user, taskID, time.Now().UTC())
	if err != nil {
		return err
	}
	if !cmd.Update() {
		return db.ErrRecordNotFound
	}
	return tx.Commit(ctx)
}

func GetParentID(ctx context.Context, sample string, step gmodel.ELabStep) (int64, error) {
	s, ctx, err := db.GetOrCreateConnection(ctx)
	if err != nil {
		return 0, err
	}
	defer s.Release()
	rows, err := s.Conn.Query(ctx, `
SELECT ts.parent_id FROM "evogen"."lab_process_tasks_samples" ts
LEFT JOIN "evogen"."lab_process_tasks" t ON t.id = ts.task_id
WHERE 
	ts.parent_id is not null AND 
	t.step = $1 AND 
	ts.lab_id = $2
ORDER BY t.id DESC
LIMIT 1
`, step, sample)
	if err != nil {
		log.Errorf("Failed to query for parent lab task %v", err)
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		err = rows.Scan(&id)
		if err != nil {
			log.Errorf("Failed to query for parent lab task %v", err)
			return 0, err
		}
		return id, nil
	}
	return 0, db.ErrRecordNotFound
}

func QueryPreviousStageTasksForSamples(ctx context.Context, labIds []string, prevStep gmodel.ELabStep) (map[string][]*task.LabTask, error) {
	labTasksLabIdMap := map[string][]*task.LabTask{}
	labTasksFilter := entities.Filter{Fields: map[string][]string{"labId": labIds, "state": {gmodel.ELabOperationStatusDone.String()}}}
	labTasksCtx := context.WithValue(ctx, base.CtxKeyFilter, labTasksFilter)
	steps := []gmodel.ELabStep{prevStep}
	labTasks, _, err := Query(labTasksCtx, steps, -1, 0)
	if err != nil {
		return nil, err
	}

	labTasksWithSamples := make([]*task.LabTask, 0, len(labTasks))
	for _, labTask := range labTasks {
		labTaskWithSamples, err := LoadTaskWithSamples(ctx, labTask.ID)
		if err != nil {
			return nil, err
		}
		labTasksWithSamples = append(labTasksWithSamples, labTaskWithSamples)
	}

	for _, labTask := range labTasksWithSamples {
		for _, sample := range labTask.Samples {
			found := false
			sampleId := sample.SampleID.String()
			for _, labId := range labIds {
				if sampleId == labId {
					found = true
					break
				}
			}
			if !found {
				continue
			}

			labTaskCopy := *labTask
			labTaskCopy.Samples = []task.TaskSample{sample}
			labTasksLabIdMap[sampleId] = append(labTasksLabIdMap[sampleId], &labTaskCopy)
		}
	}

	return labTasksLabIdMap, nil
}

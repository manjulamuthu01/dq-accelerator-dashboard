# Implementation Plan: Projects & Datasets Management

## Context & Architecture Decisions

### Codebase Overview
- **Frontend**: Dash + Dash Bootstrap Components + Plotly
- **Backend**: Python with dual-mode db_adapter (Databricks SQL or local SQLite)
- **Database**: dq_project_config, dq_table_config with metadata tables
- **Existing Patterns**: Page-based routing in dash_app.py, callback-driven interactivity, query_df/execute_cmd for DB operations
- **Schema**: Databricks Unity Catalog with fallback to local SQLite

### Design Decisions

1. **Backend Functions in db_adapter.py**: Following existing patterns (query_df, execute_cmd), add 8 new functions for CRUD operations. Each returns structured {status, message, data} responses.

2. **Table Connectivity Testing**: Use Spark/SQL SELECT COUNT(*) to verify table exists and is accessible. For local SQLite, verify table schema directly. Return clear success/error messages.

3. **Configuration Validation**: Expand accelerator/config.py with a new ConfigValidator class to parse catalog.schema.table safely, validate required fields, and return structured validation errors.

4. **Frontend Modals**: Enhance dash_app.py page_management() with Add/Edit/View modals. Use dcc.Store for edit context, pattern matching for dynamic button IDs (View/Edit/Disable/Enable per project).

5. **State Management**: Use dcc.Store + pattern-matching callbacks for modal state. Refresh management table via "project-refresh-store" trigger after any mutation (Add/Edit/Toggle).

6. **Error Handling**: All db_adapter functions catch exceptions, log to console, and return {status: "error", message: "...", data: {}}. Frontend displays toast notifications with severity levels.

7. **Data Flow**: Create Project → auto-open Register Dataset modal → Test Connectivity → Register Dataset → refresh projects table → enable View/Edit/Disable.

---

## Implementation Plan

### 1. Expand accelerator/config.py: Add ConfigValidator Class
Add a new ConfigValidator class to validate user inputs and parse table references safely.

**Rationale**: Centralize configuration validation following the existing Config class pattern in accelerator/config.py. Keeps validation logic reusable and separate from db_adapter CRUD logic.

**Files**: accelerator/config.py
**Verify**: `python -c "from accelerator import ConfigValidator; print('Import successful')"` — no errors.

---

### 2. Add Backend CRUD Functions to db_adapter.py
Implement 8 new backend functions following existing db_adapter patterns (query_df, execute_cmd, exception handling).

**Rationale**: db_adapter already handles dual-mode (Databricks/SQLite), SQL parameter binding, and structured DataFrame returns. New functions follow the same error-handling model: try Databricks, fall back to local, return {status, message, data}.

**Files**: db_adapter.py
**Functions to add**:
- `create_project(project_name, description, owner, domain)` → {status, message, data: {project_id, project_name}}
- `create_dataset(project_id, dataset_name, source_table, target_table, business_key)` → {status, message, data: {dataset_id}}
- `test_table_connectivity(catalog, schema, table)` → {status, message, data: {accessible: bool, table_exists: bool}}
- `get_project_details(project_id)` → {status, message, data: {project_info, datasets_list, last_execution, dq_score}}
- `get_all_projects()` → {status, message, data: {projects_df}}
- `update_project(project_id, project_name, description, owner)` → {status, message, data: {}}
- `toggle_project_active_status(project_id)` → {status, message, data: {new_active_status}}
- `get_dataset_rules(dataset_id)` → {status, message, data: {rules_df}}

**Verify**: `pytest test_phase8.py -v -k "test_create" 2>&1 | grep -E "(PASS|FAIL|ERROR)"` — all new CRUD tests pass locally.

---

### 3. Modify dash_app.py: Enhance page_management() with Add Project & Edit Modals
Update the page_management() function to include fully functional modals for Add Project, Register Dataset, Edit Project, and project actions (View/Edit/Disable/Enable).

**Rationale**: The existing modals are already defined (modal-add-project, modal-edit-project, modal-view-project) but are not wired to backend functions or state management. Enhance them to open on button clicks and call new backend functions.

**Files**: dash_app.py, _build_management_table_rows() and page_management()
**Changes**:
- Ensure Add Project modal opens when "Add Project" button clicked
- Add Register Dataset modal (currently not visible) to open after project creation
- Ensure View/Edit/Disable/Enable buttons trigger correct callbacks
- Wire project-refresh-store dcc.Store to trigger table refresh after mutations

**Verify**: Open http://localhost:8050/management in browser — "Add Project" button opens modal, "View/Edit/Disable/Enable" links are clickable (no JS errors in console).

---

### 4. Implement Management Page Callbacks in dash_app.py
Add / expand callbacks to wire modals to backend functions and handle form submissions.

**Rationale**: Callbacks already exist (handle_add_project, handle_edit_project, handle_view_project, handle_table_actions) but may not fully implement all backend calls or error handling. Ensure each calls the new db_adapter functions and updates UI accordingly.

**Files**: dash_app.py
**Callbacks**:
- `handle_add_project`: POST project details to db_adapter.create_project() → open Register Dataset modal
- `handle_test_connectivity`: POST table refs to db_adapter.test_table_connectivity() → show success/error toast
- `handle_register_dataset`: POST dataset details to db_adapter.create_dataset() → refresh management table
- `handle_view_project`: Load project details via db_adapter.get_project_details() → render modal body
- `handle_edit_project`: Load current values, save edits via db_adapter.update_project() → refresh table
- `handle_toggle_project`: POST to db_adapter.toggle_project_active_status() → refresh table
- `refresh_management_table`: Triggered by project-refresh-store, rebuild table rows via _build_management_table_rows()

**Verify**: 
- Create a project via modal → verify it appears in projects table
- Click View → verify project details page loads
- Click Edit → verify form pre-fills and save updates the table
- Click Disable → verify status badge changes to "Inactive"
- Click Enable → verify status badge changes to "Active"

---

### 5. Add Backend Function: db_adapter.create_project()
Implement project creation with INSERT into dq_project_config table.

**Rationale**: First CRUD operation. Establishes pattern for all other functions: validate inputs, execute SQL, handle exceptions, return structured response.

**Files**: db_adapter.py
**SQL**: `INSERT INTO dq_project_config (project_name, description, owner, active, created_at, updated_at) VALUES (?, ?, ?, 1, NOW(), NOW())`
**Validation**: 
- project_name required (non-empty string)
- owner required
- description optional
- Check for duplicate project_name before insert

**Response Shape**:
```python
{
  "status": "success|error",
  "message": "Project 'Banking' created successfully|Error: duplicate project name|Error: invalid input",
  "data": {"project_id": 123, "project_name": "Banking"}
}
```

**Verify**: `pytest test_phase8.py::test_create_project -v` — passes, returns correct project_id.

---

### 6. Add Backend Function: db_adapter.create_dataset()
Implement dataset registration with INSERT into dq_table_config table.

**Rationale**: Second CRUD operation, depends on project creation. Stores source/target table refs and business key.

**Files**: db_adapter.py
**SQL**: `INSERT INTO dq_table_config (project_id, dataset_name, source_table, target_table, business_key, active, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 1, NOW(), NOW())`
**Validation**: 
- project_id must exist (FK constraint)
- source_table and business_key required
- Parse source_table as catalog.schema.table

**Response Shape**:
```python
{
  "status": "success|error",
  "message": "Dataset registered|Error: project_id not found",
  "data": {"dataset_id": 456}
}
```

**Verify**: `pytest test_phase8.py::test_create_dataset -v` — passes, foreign key constraint enforced.

---

### 7. Add Backend Function: db_adapter.test_table_connectivity()
Implement table connectivity test via Spark/SQL query or SQLite schema check.

**Rationale**: Validate table existence and user access before saving dataset config. Provides early feedback to user (success/error toast).

**Files**: db_adapter.py
**Implementation**:
- **Databricks mode**: Execute `SELECT COUNT(*) FROM {catalog}.{schema}.{table}` via Spark/SQL cursor
- **SQLite mode**: Check if table exists via SQLite schema query
- Catch exceptions (table not found, access denied, etc.)

**Response Shape**:
```python
{
  "status": "success|error",
  "message": "Table is accessible (count: 12345 rows)|Error: table not found|Error: access denied",
  "data": {"accessible": true/false, "table_exists": true/false, "row_count": 12345}
}
```

**Verify**: `pytest test_phase8.py::test_connectivity -v` — passes for existing tables, fails gracefully for non-existent tables.

---

### 8. Add Backend Function: db_adapter.get_project_details()
Implement rich project view with project info, datasets, rules, last execution, and DQ score.

**Rationale**: Supports View Project modal. Joins dq_project_config, dq_table_config, dq_rules, dq_execution_summary to show complete project picture.

**Files**: db_adapter.py
**SQL**:
- SELECT from dq_project_config WHERE project_id = ?
- SELECT from dq_table_config WHERE project_id = ? (all datasets)
- SELECT from dq_rules WHERE project_id = ? GROUP BY dataset_id (active rules per dataset)
- SELECT * FROM dq_execution_summary WHERE project_id = ? ORDER BY started_at DESC LIMIT 1 (last execution)

**Response Shape**:
```python
{
  "status": "success|error",
  "message": "...",
  "data": {
    "project": {"project_id": 1, "project_name": "...", "owner": "...", "active": true, "created_at": "..."},
    "datasets": [{"dataset_id": 10, "dataset_name": "...", "source_table": "...", "business_key": "..."}],
    "rules": [{"rule_id": 100, "rule_name": "...", "dataset_id": 10}],
    "last_execution": {"execution_id": "...", "overall_status": "PASS", "dq_score": 98.5, "started_at": "..."},
    "dq_score": 98.5
  }
}
```

**Verify**: `pytest test_phase8.py::test_get_project_details -v` — passes, returns all nested data structures correctly.

---

### 9. Add Backend Function: db_adapter.get_all_projects()
Implement projects list with dataset count.

**Rationale**: Powers management table display. Uses existing pattern (query_df) but adds dataset count aggregation.

**Files**: db_adapter.py
**SQL**:
```sql
SELECT p.project_id, p.project_name, p.description, p.owner, p.active, p.created_at,
       COUNT(DISTINCT d.dataset_id) AS dataset_count
FROM dq_project_config p
LEFT JOIN dq_table_config d ON p.project_id = d.project_id
GROUP BY p.project_id, p.project_name, p.description, p.owner, p.active, p.created_at
ORDER BY p.created_at DESC
```

**Response Shape**: Returns DataFrame (pandas) with columns: project_id, project_name, description, owner, active, created_at, dataset_count.

**Verify**: `pytest test_phase8.py::test_get_all_projects -v` — passes, DataFrame has all expected columns, dataset_count aggregates correctly.

---

### 10. Add Backend Function: db_adapter.update_project()
Implement project edit (update project_name, description, owner).

**Rationale**: Supports Edit modal. Updates dq_project_config with new values.

**Files**: db_adapter.py
**SQL**: `UPDATE dq_project_config SET project_name = ?, description = ?, owner = ?, updated_at = NOW() WHERE project_id = ?`
**Validation**:
- project_id must exist
- new project_name must be unique (if changed)

**Response Shape**:
```python
{
  "status": "success|error",
  "message": "Project updated|Error: duplicate project_name",
  "data": {}
}
```

**Verify**: `pytest test_phase8.py::test_update_project -v` — passes, duplicate name check works.

---

### 11. Add Backend Function: db_adapter.toggle_project_active_status()
Implement Disable/Enable toggle (flip active flag in dq_project_config).

**Rationale**: Supports Disable/Enable buttons. Toggles active status and prevents new executions for inactive projects.

**Files**: db_adapter.py
**SQL**: `UPDATE dq_project_config SET active = NOT active, updated_at = NOW() WHERE project_id = ?`

**Response Shape**:
```python
{
  "status": "success|error",
  "message": "Project disabled|Project enabled",
  "data": {"new_active_status": true/false}
}
```

**Verify**: `pytest test_phase8.py::test_toggle_status -v` — passes, status flips correctly.

---

### 12. Add Backend Function: db_adapter.get_dataset_rules()
Implement rules list for a specific dataset.

**Rationale**: Supports View Project modal to show active DQ rules for each dataset.

**Files**: db_adapter.py
**SQL**:
```sql
SELECT rule_id, rule_name, template_name, category, severity, column_name, threshold
FROM dq_rules
WHERE dataset_id = ? AND active = 1
ORDER BY created_at DESC
```

**Response Shape**: Returns DataFrame with columns: rule_id, rule_name, template_name, category, severity, column_name, threshold.

**Verify**: `pytest test_phase8.py::test_get_dataset_rules -v` — passes, returns only active rules for specified dataset.

---

### 13. Enhance dash_app.py Callback: handle_add_project()
Wire Add Project form to db_adapter.create_project(), then auto-open Register Dataset modal.

**Rationale**: Current callback may not fully call backend functions or chain to dataset modal. Ensure:
1. Validate inputs on frontend (required fields)
2. Call db_adapter.create_project()
3. On success: store project_id in dcc.Store, open Register Dataset modal, show success toast
4. On error: show error alert in modal, keep modal open

**Files**: dash_app.py
**Verify**: 
- Fill Add Project form → click "Create Project" → success toast appears → Register Dataset modal opens
- Leave required field blank → click "Create Project" → error alert appears in modal

---

### 14. Enhance dash_app.py Callback: handle_test_connectivity()
Wire Test Connectivity button to db_adapter.test_table_connectivity().

**Rationale**: Current callback may not fully call backend validation. Ensure:
1. Parse source_table (catalog.schema.table) from input
2. Call db_adapter.test_table_connectivity()
3. Show success (green badge) or error (red badge) in modal

**Files**: dash_app.py
**Verify**: 
- Enter valid table ref → click "Test Connectivity" → green success badge appears
- Enter invalid table ref → click "Test Connectivity" → red error badge appears

---

### 15. Enhance dash_app.py Callback: handle_view_project()
Wire View button to db_adapter.get_project_details(), render project details modal.

**Rationale**: Current callback may not fully load all nested project data. Ensure:
1. Extract project_id from pattern-matched button click
2. Call db_adapter.get_project_details(project_id)
3. Render rich modal body with:
   - Project info card (name, owner, domain, created_at)
   - Datasets table (dataset_name, source, target, business_key, active status, actions)
   - Active DQ rules list
   - Last execution status
   - Historical DQ score (from dq_execution_summary)
4. Include "Configure DQ Rules" button to navigate to /rules

**Files**: dash_app.py
**Verify**: 
- Click View on a project → modal opens with all nested data visible
- Datasets table shows all datasets for that project
- Rules list shows active rules

---

### 16. Enhance dash_app.py Callback: handle_edit_project()
Wire Edit button to load project data, then save updates via db_adapter.update_project().

**Rationale**: Current callback may not fully pre-fill form or save changes. Ensure:
1. Extract project_id from pattern-matched button click
2. Load current values via db_adapter.get_project_details() or query_df
3. Pre-fill modal form fields
4. On Save: call db_adapter.update_project() with new values
5. On success: close modal, refresh management table via project-refresh-store, show success toast
6. On error: show error alert in modal

**Files**: dash_app.py
**Verify**: 
- Click Edit on a project → modal opens with current values pre-filled
- Change values → click "Save Changes" → management table updates

---

### 17. Enhance dash_app.py Callback: handle_table_actions()
Wire Toggle (Disable/Enable) buttons to db_adapter.toggle_project_active_status().

**Rationale**: Current callback may not fully handle toggle. Ensure:
1. Extract project_id from pattern-matched button click
2. Call db_adapter.toggle_project_active_status(project_id)
3. On success: refresh management table, show confirmation toast ("Project disabled" / "Project enabled")
4. On error: show error toast

**Files**: dash_app.py
**Verify**: 
- Active project → click Disable → status changes to "Inactive", table updates
- Inactive project → click Enable → status changes to "Active", table updates

---

### 18. Add Table Refresh Callback in dash_app.py
Implement refresh_management_table() callback triggered by project-refresh-store.

**Rationale**: After any mutation (Add/Edit/Toggle), update the management table without full page reload.

**Files**: dash_app.py
**Callback**:
```python
@app.callback(
    Output("management-table-body", "children"),
    Input("project-refresh-store", "data"),
    prevent_initial_call=True
)
def refresh_management_table(refresh_ts):
    projects_df = db_adapter.get_all_projects()
    return _build_management_table_rows(projects_df)
```

**Verify**: 
- Create a project → management table updates without page reload
- Edit a project → management table updates in real-time

---

### 19. Test All Modals and Error States
Verify modals open/close correctly, buttons are clickable, and error messages display clearly.

**Rationale**: Ensures user experience is smooth and error feedback is actionable.

**Files**: dash_app.py (manual testing)
**Manual Tests**:
- Open Add Project modal → all form fields visible and editable
- Open Edit Project modal → form pre-fills with current values
- Open View Project modal → all nested data (datasets, rules, execution) visible
- Test connectivity with invalid table → error badge appears with clear message
- Submit form with required fields empty → validation error shows
- Submit form with valid data → success toast and modal closes

---

### 20. Integration Test: End-to-End Project & Dataset Workflow
Test complete flow: Add Project → Register Dataset → Test Connectivity → View → Edit → Disable/Enable.

**Rationale**: Ensures all components work together seamlessly.

**Files**: test_phase8.py (new integration test)
**Test Steps**:
1. Create project via backend function
2. Create dataset via backend function
3. Test connectivity via backend function (verify success)
4. Retrieve project details via backend function (verify all nested data)
5. Update project via backend function (verify changes)
6. Toggle project status via backend function (verify status flip)
7. Retrieve updated project (verify all changes persisted)

**Verify**: `pytest test_phase8.py::test_end_to_end_project_workflow -v` — passes all steps, no errors.

---

## Verification Steps (Global)

After all changes, run:

1. **Syntax Check**: `python -m py_compile dash_app.py db_adapter.py accelerator/config.py` — no compile errors
2. **Import Check**: `python -c "import dash_app; import db_adapter; from accelerator import ConfigValidator"` — no import errors
3. **Dash App Start**: `python dash_app.py` — app starts without errors (check http://localhost:8050)
4. **Navigation**: Click through all pages in sidebar — all pages load without errors
5. **Project Management Page**: 
   - Click "Add Project" → modal opens
   - Fill form → click "Create Project" → success toast → table updates
   - Click "View" on a project → details modal shows all data
   - Click "Edit" on a project → edit modal shows current values
   - Click "Disable" → status changes to "Inactive"
   - Click "Enable" → status changes to "Active"
6. **Backend Tests**: `pytest test_phase8.py -v` — all existing + new tests pass

---

## File Summary

| File | Changes |
|------|---------|
| accelerator/config.py | Add ConfigValidator class with methods: validate_project(), validate_dataset(), parse_table_ref() |
| db_adapter.py | Add 8 CRUD functions: create_project, create_dataset, test_table_connectivity, get_project_details, get_all_projects, update_project, toggle_project_active_status, get_dataset_rules |
| dash_app.py | Enhance page_management(), _build_management_table_rows(); wire/expand callbacks: handle_add_project, handle_test_connectivity, handle_view_project, handle_edit_project, handle_table_actions, refresh_management_table |
| test_phase8.py | Add integration test: test_end_to_end_project_workflow |

---

## Notes

- All responses from backend functions follow {status, message, data} structure for consistent error handling.
- All callbacks use `prevent_initial_call=True` to avoid unnecessary execution on page load.
- Pattern-matching IDs ({"type": "btn-view-proj", "id": project_id}) enable dynamic button handling.
- project-refresh-store dcc.Store acts as a refresh signal — updated after each mutation.
- Existing test patterns (Phase 6-8) provide templates for new backend function tests.
- No breaking changes to existing pages (Overview, Rules, Execution, Results, Reconciliation, Alerts, Audit).

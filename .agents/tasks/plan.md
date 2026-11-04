# Implementation Plan: Rule Library / Rule Configuration Page (Phase 8)

## Overview
Complete, production-ready Rule Library / Rule Configuration page for the Dynamic Data Quality Accelerator. The system allows users to select projects, datasets, and predefined rule templates; configure rules with parameters; preview generated SQL; and save rules to the database with full audit logging.

**Key Constraint**: All SQL generation uses controlled templates from `dq_rule_templates` with injection-safe placeholder replacement. No raw user SQL is executed.

---

## Backend Functions (db_adapter.py)

### 1. Get Projects for Dropdown
- **Function**: `get_projects_for_dropdown() -> pd.DataFrame`
- **What it does**: Returns all active projects from `dq_project_config` with columns `project_id`, `project_name`, `description`.
- **Query**: `SELECT project_id, project_name, description FROM dq_project_config WHERE active = true ORDER BY project_name`
- **Files**: db_adapter.py
- **Verify**: Unit test calls the function and confirms it returns a non-empty DataFrame with correct columns.

### 2. Get Datasets for Project (Filtered)
- **Function**: `get_datasets_for_project(project_name: Optional[str] = None) -> List[str]`
- **What it does**: Already exists in codebase but needs to be verified and enhanced if necessary. Returns dataset names for a given project.
- **Current location**: Line ~1005 in db_adapter.py
- **Verify**: Confirm it filters by project_id from the project lookup, returns only active datasets.

### 3. Get Rule Templates
- **Function**: `get_rule_templates() -> pd.DataFrame`
- **What it does**: Retrieves all active rule templates from `dq_rule_templates` with columns: `template_id`, `template_name`, `category`, `description`, `requires_column`, `requires_target`, `result_type`, `template_sql`.
- **Query**: `SELECT template_id, template_name, category, description, requires_column, requires_target, result_type, template_sql FROM dq_rule_templates WHERE is_active = true ORDER BY template_name`
- **Files**: db_adapter.py
- **Verify**: Confirm all 14 templates load from the DB; check that `template_sql` contains placeholders like `{source_table}`, `{column_name}`, etc.

### 4. Get Configured Rules for Project + Dataset
- **Function**: `get_configured_rules(project_id: int, dataset_id: int) -> pd.DataFrame`
- **What it does**: Fetches all rules (active and inactive) for a specific project + dataset combination. Returns: `rule_id`, `rule_name`, `column_name`, `threshold`, `severity`, `active`, `created_at`, `template_name`.
- **Query**: 
  ```sql
  SELECT rule_id, rule_name, column_name, threshold, severity, active, created_at, template_name
  FROM dq_rules
  WHERE project_id = ? AND dataset_id = ?
  ORDER BY created_at DESC
  ```
- **Files**: db_adapter.py
- **Verify**: Call with a known project/dataset pair (e.g., Hospital, source_table) and confirm it returns all existing rules for that combination.

### 5. Get Rule Details (for Edit Modal)
- **Function**: `get_rule_details(rule_id: int) -> Dict[str, Any]`
- **What it does**: Fetches a single rule for pre-filling the edit form. Returns all fields: `rule_id`, `rule_name`, `project_id`, `dataset_id`, `template_name`, `column_name`, `threshold`, `severity`, `active`.
- **Query**:
  ```sql
  SELECT rule_id, rule_name, project_id, dataset_id, template_name, column_name, threshold, severity, active
  FROM dq_rules
  WHERE rule_id = ?
  ```
- **Files**: db_adapter.py
- **Verify**: Fetch a known rule ID and confirm all fields are populated correctly.

### 6. Generate SQL Preview
- **Function**: `generate_sql_preview(project_id: int, dataset_id: int, template_name: str, column_name: Optional[str] = None) -> str`
- **What it does**: 
  - Looks up the dataset's source table, target table, and business key from `dq_table_config`.
  - Looks up the rule template's `template_sql` from `dq_rule_templates`.
  - Safely replaces all placeholders: `{source_table}` → full catalog.schema.table, `{target_table}` → full catalog.schema.table, `{column_name}` → user-provided or business key, `{rule_parameter}` → business key.
  - Returns the rendered SQL string.
- **Validation**: 
  - Source table path must be catalog.schema.table format.
  - Column names are user input but not directly interpolated; the template controls SQL structure.
- **Files**: db_adapter.py
- **Verify**: Call with NULL_COUNT template, Hospital project, source_table dataset, column "patient_name". Confirm output is: `SELECT COUNT(*) AS failure_count FROM hospital_catalog.healthcare.patients_source WHERE patient_name IS NULL`

### 7. Validate and Save Rule
- **Function**: `save_rule(project_id: int, dataset_id: int, template_name: str, rule_name: str, column_name: Optional[str], threshold: float, severity: str, active: bool, user: str = "admin") -> Dict[str, Any]`
- **What it does**:
  - Validates: project_id and dataset_id exist and are active.
  - Validates: template_name exists in `dq_rule_templates`.
  - Validates: column_name is provided if template requires it.
  - Validates: severity is one of `["Critical", "High", "Medium", "Low"]`.
  - Validates: threshold is numeric.
  - Checks for duplicate rule: same project + dataset + template + column (optional).
  - If all validations pass:
    - Generate a unique `rule_id` (auto-increment).
    - INSERT into `dq_rules` with all fields.
    - Log the action in `dq_config_audit` with action="INSERT", entity_type="dq_rules", changed_by=user.
  - Returns: `{"success": bool, "message": str, "rule_id": int}` or error details.
- **Error Responses**: 
  - `{"success": false, "message": "Project not found or inactive"}`
  - `{"success": false, "message": "Dataset not found or inactive"}`
  - `{"success": false, "message": "Template not found"}`
  - `{"success": false, "message": "Column required for this template"}`
  - `{"success": false, "message": "Invalid severity"}`
  - `{"success": false, "message": "Duplicate rule"}`
- **Files**: db_adapter.py
- **Verify**: Call with valid inputs; confirm INSERT succeeds and audit log records the action. Then call with duplicate inputs; confirm it returns error.

### 8. Update Existing Rule
- **Function**: `update_rule(rule_id: int, rule_name: str, column_name: Optional[str], threshold: float, severity: str, active: bool, user: str = "admin") -> Dict[str, Any]`
- **What it does**:
  - Validates inputs (severity, threshold).
  - UPDATEs `dq_rules` table where rule_id = ?.
  - Logs the action in `dq_config_audit` with old and new values.
  - Returns: `{"success": bool, "message": str}`.
- **Files**: db_adapter.py
- **Verify**: Fetch a rule, modify its threshold/severity, update it, and confirm the DB reflects the change.

### 9. Delete or Deactivate Rule
- **Function**: `delete_rule(rule_id: int, user: str = "admin", hard_delete: bool = False) -> Dict[str, Any]`
- **What it does**:
  - If `hard_delete=False` (default): SET active=false on the rule. Preserves all execution history in `dq_results`.
  - If `hard_delete=True`: DELETE from `dq_rules`. (Use with caution; only when explicitly requested.)
  - Logs the action in `dq_config_audit`.
  - Returns: `{"success": bool, "message": str}`.
- **Critical**: Deleting or deactivating a rule must NEVER touch `dq_results`. Execution history is immutable.
- **Files**: db_adapter.py
- **Verify**: Deactivate a rule; confirm it is marked inactive but previous results remain in `dq_results`.

### 10. Toggle Rule Active Status
- **Function**: `toggle_rule_active(rule_id: int, user: str = "admin") -> Dict[str, Any]`
- **What it does**:
  - Fetches current active flag.
  - Flips it (true → false, false → true).
  - UPDATEs `dq_rules`.
  - Logs the action in `dq_config_audit`.
  - Returns: `{"success": bool, "message": str, "new_status": bool}`.
- **Files**: db_adapter.py
- **Verify**: Toggle a rule on/off and confirm the flag flips correctly.

---

## Frontend Callbacks (dash_app.py)

The page_rules() function and its associated callbacks handle all user interactions. All callbacks operate on the existing Rule Configuration page layout (HTML structure already present in page_rules() function starting at line ~869).

### Callback 1: Project Selection → Dataset Dropdown Refresh
- **Trigger**: `Input("rule-select-project", "value")` (project dropdown change)
- **Output**: `Output("rule-select-dataset", "options")` (dataset dropdown options)
- **Logic**:
  1. Get the selected project_name from the input.
  2. Call `db_adapter.get_projects_for_dropdown()` to find project_id.
  3. Call `db_adapter.get_datasets_for_project(project_name)` to fetch dataset names.
  4. Return list of dicts: `[{"label": ds, "value": ds} for ds in datasets]`.
  5. Set dataset value to the first dataset (or "source_table" if available).
- **Files**: dash_app.py (add/modify callback ~line 2090)
- **Verify**: Select "Hospital", confirm dataset dropdown shows "source_table". Select "Banking", confirm it shows "customer".

### Callback 2: Dataset Selection → Refresh Configured Rules Table
- **Trigger**: 
  - `Input("rule-select-project", "value")`
  - `Input("rule-select-dataset", "value")`
- **Output**: `Output("configured-rules-table-body", "children")` (table body rows)
- **Logic**:
  1. Get selected project_name and dataset_name.
  2. Look up project_id and dataset_id from the DB.
  3. Call `db_adapter.get_configured_rules(project_id, dataset_id)`.
  4. For each row, build an `<tr>` with: rule_name, column_name, threshold, severity, active badge, created_at, and Actions (View SQL, Edit, Delete, Toggle).
  5. Return the list of `<tr>` elements.
- **Files**: dash_app.py
- **Verify**: Select Hospital + source_table; confirm the table populates with the configured rules. Change dataset; confirm table updates.

### Callback 3: Template Tile Click → Populate Configuration Form
- **Trigger**: `Input({"type": "template-tile", "index": ALL}, "n_clicks")`
- **Outputs**:
  - `Output("rule-input-name", "value")` (rule name)
  - `Output("rule-input-column", "placeholder")`  (column placeholder)
  - `Output("rule-input-threshold", "value")` (default threshold)
  - `Output("rule-input-threshold", "placeholder")` (threshold help text)
- **Logic**:
  1. Identify which template tile was clicked using `ctx.triggered`.
  2. Look up the template from `db_adapter.get_rule_templates()`.
  3. Pre-fill:
     - Rule Name: template_name (editable).
     - Column placeholder: "Required: column name" or "Optional" based on template.requires_column.
     - Threshold: template's default_threshold.
     - Severity: default to "High".
     - Active: default to True.
  4. Show/hide column input based on requires_column.
  5. Show/hide target table selection based on requires_target.
- **Files**: dash_app.py
- **Verify**: Click NULL_COUNT tile; confirm form shows "NULL_COUNT" as rule name and column input is visible. Click ROW_COUNT; confirm column input is hidden.

### Callback 4: Preview SQL Button → Modal with Generated SQL
- **Trigger**: `Input("btn-preview-sql", "n_clicks")`
- **Outputs**:
  - `Output("modal-preview-sql", "is_open")`
  - `Output("preview-sql-code", "children")`
- **States**:
  - `State("rule-select-project", "value")` (selected project)
  - `State("rule-select-dataset", "value")` (selected dataset)
  - `State("rule-input-name", "value")` (rule name, used to infer template)
  - `State("rule-input-column", "value")` (column name)
- **Logic**:
  1. Infer template_name from rule name (or track it in a hidden dcc.Store).
  2. Get project_id and dataset_id.
  3. Call `db_adapter.generate_sql_preview(project_id, dataset_id, template_name, column_name)`.
  4. Display SQL in a `<pre>` block inside the modal.
  5. Set `is_open=True` to show the modal.
- **Files**: dash_app.py
- **Verify**: Select Hospital + source_table, choose NULL_COUNT, enter "patient_name", click Preview SQL. Modal opens and shows the correct SQL with full table path.

### Callback 5: Save Rule Button → Validate, Insert, Show Toast, Refresh Table
- **Trigger**: `Input("btn-save-rule", "n_clicks")`
- **Outputs**:
  - `Output("global-toast", "is_open")`
  - `Output("global-toast", "children")`
  - `Output("configured-rules-table-body", "children")`  (refresh table)
  - `Output("rule-input-name", "value")` (reset form)
  - `Output("rule-input-column", "value")` (reset form)
  - `Output("rule-input-threshold", "value")` (reset form)
  - `Output("rule-select-severity", "value")` (reset form)
  - `Output("rule-switch-active", "value")` (reset form)
- **States**: All form fields (project, dataset, rule name, column, threshold, severity, active).
- **Logic**:
  1. Validate all inputs on the client side (non-empty rule name, valid threshold, severity in list).
  2. Look up project_id and dataset_id.
  3. Infer template_name from rule name (or use hidden Store).
  4. Call `db_adapter.save_rule(project_id, dataset_id, template_name, rule_name, column_name, threshold, severity, active, user="admin")`.
  5. If success:
     - Show success toast: "Rule saved successfully!".
     - Refresh the configured rules table by calling Callback 2.
     - Clear the form.
  6. If error:
     - Show error toast with the error message.
     - Do NOT refresh the table or clear the form; let user correct and retry.
- **Files**: dash_app.py
- **Verify**: Fill in form (Hospital, source_table, NULL_COUNT, patient_name, 0, High), click Save. Toast shows success, table updates. Try to save a duplicate; toast shows error.

### Callback 6: Edit Button → Open Modal with Pre-filled Values
- **Trigger**: `Input({"type": "btn-edit-rule", "index": ALL}, "n_clicks")`
- **Outputs**:
  - `Output("modal-edit-rule", "is_open")`
  - `Output("edit-rule-id", "data")` (store rule_id for later)
  - `Output("edit-rule-name", "value")`
  - `Output("edit-rule-column", "value")`
  - `Output("edit-rule-threshold", "value")`
  - `Output("edit-rule-severity", "value")`
  - `Output("edit-rule-switch-active", "value")`
- **Logic**:
  1. Extract rule_id from the clicked button's index.
  2. Call `db_adapter.get_rule_details(rule_id)`.
  3. Populate the modal form with the fetched values.
  4. Set `is_open=True` to show the modal.
- **Files**: dash_app.py
- **Verify**: Click Edit on an existing rule; modal opens with pre-filled values.

### Callback 7: Save Changes in Edit Modal
- **Trigger**: `Input("btn-submit-edit-rule", "n_clicks")`
- **Outputs**:
  - `Output("modal-edit-rule", "is_open")`
  - `Output("global-toast", "is_open")`
  - `Output("global-toast", "children")`
  - `Output("configured-rules-table-body", "children")`  (refresh table)
- **States**: All edit form fields + `State("edit-rule-id", "data")`.
- **Logic**:
  1. Validate inputs.
  2. Call `db_adapter.update_rule(rule_id, rule_name, column_name, threshold, severity, active, user="admin")`.
  3. If success: close modal, show success toast, refresh table.
  4. If error: show error toast, keep modal open.
- **Files**: dash_app.py
- **Verify**: Edit a rule (change threshold from 0 to 5), save. Table updates and rule now shows new threshold.

### Callback 8: Delete Button → Confirmation, Call Backend, Refresh Table
- **Trigger**: `Input({"type": "btn-delete-rule", "index": ALL}, "n_clicks")`
- **Outputs**:
  - `Output("modal-delete-confirm", "is_open")`  (show confirmation modal)
  - `Output("delete-rule-id", "data")` (store rule_id)
- **Then**, after user confirms:
  - `Input("btn-confirm-delete", "n_clicks")`
  - **Outputs**:
    - `Output("modal-delete-confirm", "is_open")` (close modal)
    - `Output("global-toast", "is_open")`
    - `Output("global-toast", "children")`
    - `Output("configured-rules-table-body", "children")` (refresh table)
- **Logic**:
  1. On delete button click: show confirmation modal.
  2. On confirm click: call `db_adapter.delete_rule(rule_id, hard_delete=False)` (soft delete; marks inactive).
  3. Show success toast, refresh table.
- **Files**: dash_app.py
- **Verify**: Click Delete on a rule; confirmation modal appears. Click Confirm; table updates and rule is no longer visible (or marked inactive). Verify `dq_results` still contains the rule's execution history.

### Callback 9: Toggle Button → Flip Active Flag, Refresh Table
- **Trigger**: `Input({"type": "btn-toggle-rule", "index": ALL}, "n_clicks")`
- **Outputs**:
  - `Output("global-toast", "is_open")`
  - `Output("global-toast", "children")`
  - `Output("configured-rules-table-body", "children")` (refresh table)
- **Logic**:
  1. Extract rule_id from button index.
  2. Call `db_adapter.toggle_rule_active(rule_id, user="admin")`.
  3. Show success toast with new status.
  4. Refresh the table.
- **Files**: dash_app.py
- **Verify**: Click Toggle on an active rule; it becomes inactive (show "Inactive" badge). Click again; it becomes active again.

### Callback 10: Search / Filter Rules Table
- **Trigger**: `Input("input-search-rules", "value")`
- **Output**: `Output("configured-rules-table-body", "children")`
- **Logic**:
  1. Get all rules for the selected project + dataset (as in Callback 2).
  2. Filter rows by search term in rule_name or column_name (case-insensitive substring match).
  3. Return filtered `<tr>` list.
- **Files**: dash_app.py
- **Verify**: Type "patient" in search; only rules with "patient" in name or column show up.

---

## Safe SQL Template Rendering (rule_engine.py)

### Enhancement: Safe Template Rendering Function
- **Function**: `render_sql_safe(template_name: str, source_table: str, target_table: Optional[str], column_name: Optional[str], rule_parameter: Optional[str], threshold: Optional[float]) -> str`
- **What it does**:
  1. Validates that template_name exists in RULE_TEMPLATES.
  2. Validates that source_table, target_table, column_name, rule_parameter are non-empty identifiers (alphanumeric, underscore, dot for catalog.schema.table).
  3. Replaces placeholders using string.replace() (safe, no format strings).
  4. Returns the rendered SQL or raises ValueError if validation fails.
- **Validation Regex**: `^[a-zA-Z0-9_.]+$` for all identifiers.
- **Files**: rule_engine.py
- **Verify**: Call with valid inputs; SQL renders correctly. Call with SQL injection attempt (e.g., `"; DROP TABLE dq_rules; --"`); function rejects it with ValueError.

---

## HTML Layout (dash_app.py, page_rules function)

The page_rules() function already has most of the HTML structure in place. Ensure it includes:

1. **Header Bar** (existing):
   - Title: "Rule Configuration"
   - Project dropdown (id="rule-select-project")
   - Dataset dropdown (id="rule-select-dataset")
   - "Add Rule" button (id="btn-add-rule-trigger") — optional, can trigger form reset

2. **Left Panel: Available Rule Templates** (existing):
   - Search input (id="input-search-template")
   - Template tiles container (id="template-tiles-container")
   - Each tile is clickable: id={"type": "template-tile", "id": template_name}

3. **Right Panel: Rule Configuration Form** (existing):
   - Rule Name input (id="rule-input-name")
   - Column input (id="rule-input-column") — hidden for ROW_COUNT, SCHEMA_CHANGE, etc.
   - Threshold input (id="rule-input-threshold")
   - Severity dropdown (id="rule-select-severity")
   - Active toggle (id="rule-switch-active")
   - Preview SQL button (id="btn-preview-sql")
   - Save Rule button (id="btn-save-rule")

4. **Below Form: Configured Rules Table** (needs to be added or verified):
   - Table ID: "configured-rules-table"
   - Columns: Rule Name, Column, Threshold, Severity, Status (Active/Inactive), Created At, Actions
   - Table body ID: "configured-rules-table-body"
   - Action buttons:
     - View SQL (id={"type": "btn-view-sql-rule", "index": rule_id})
     - Edit (id={"type": "btn-edit-rule", "index": rule_id})
     - Delete (id={"type": "btn-delete-rule", "index": rule_id})
     - Toggle (id={"type": "btn-toggle-rule", "index": rule_id})
   - Search input (id="input-search-rules")

5. **Modals**:
   - SQL Preview modal (id="modal-preview-sql") — existing
   - Edit Rule modal (id="modal-edit-rule")
   - Delete Confirmation modal (id="modal-delete-confirm")

6. **Hidden Stores** (for tracking state across callbacks):
   - dcc.Store(id="edit-rule-id") — holds rule_id during edit
   - dcc.Store(id="delete-rule-id") — holds rule_id during delete
   - dcc.Store(id="current-template-name") — holds selected template name (for form population)

---

## Verification Steps

### Unit Tests (test_rule_library.py — new file)
Create `test_rule_library.py` with tests for all backend functions:

1. **test_get_projects_for_dropdown()**: Fetch projects, confirm Hospital, Banking, Logistics present.
2. **test_get_datasets_for_project()**: Fetch datasets for Hospital, confirm "source_table" present. Fetch for Banking, confirm "customer" present.
3. **test_get_rule_templates()**: Fetch templates, confirm all 14 templates loaded from DB.
4. **test_get_configured_rules()**: Fetch rules for Hospital + source_table, confirm existing rules appear.
5. **test_generate_sql_preview()**: Generate SQL for NULL_COUNT with Hospital + source_table + "patient_name", confirm output contains full table path and column.
6. **test_save_rule()**: Save a new rule with valid inputs, confirm INSERT succeeds, audit log has entry, rule_id returned. Try saving duplicate, confirm error.
7. **test_update_rule()**: Update a rule's threshold, confirm UPDATE succeeds, audit log has old/new values.
8. **test_delete_rule()**: Soft delete a rule, confirm active=false. Verify dq_results unchanged.
9. **test_toggle_rule_active()**: Toggle a rule, confirm flag flips, audit log recorded.
10. **test_sql_injection_safety()**: Attempt to inject SQL in column name, confirm template renderer rejects it.

**Run**: `pytest test_rule_library.py -v`

### Integration Test: End-to-End Rule Configuration Flow
1. Open the app at http://localhost:8050.
2. Navigate to "Rule Library" page.
3. Select project "Hospital", dataset "source_table".
4. Confirm configured rules table shows existing rules (if any).
5. Click on "NULL_COUNT" template tile.
6. Form populates: rule name="NULL_COUNT", column placeholder appears.
7. Enter column name "patient_name", threshold 0, severity "High".
8. Click "Preview SQL", modal shows: `SELECT COUNT(*) AS failure_count FROM hospital_catalog.healthcare.patients_source WHERE patient_name IS NULL`.
9. Click "Save Rule", success toast appears, table adds new row with the rule.
10. Click "Edit" on the new rule, modal shows pre-filled values, change threshold to 5, save. Table updates.
11. Click "Toggle" on the rule; badge changes to "Inactive", click again; badge changes back to "Active".
12. Click "Delete", confirmation modal appears, confirm delete. Rule disappears from table.
13. Verify in DB: `SELECT * FROM dq_results WHERE rule_id = ?` — results still present (immutable history).
14. Select project "Banking", dataset "customer", confirm a different set of rules (or empty if none configured).

**Run**: Start dev server `python dash_app.py`, manual testing as above.

### Database Verification
1. **Check dq_rules insertion**: `SELECT * FROM dq_rules WHERE rule_name LIKE 'test_%' ORDER BY created_at DESC LIMIT 5` — confirm new rules appear.
2. **Check audit logging**: `SELECT * FROM dq_config_audit WHERE entity_type = 'dq_rules' ORDER BY timestamp DESC LIMIT 10` — confirm INSERT, UPDATE, DELETE actions logged.
3. **Check history preservation**: `SELECT * FROM dq_results WHERE rule_id = <deleted_rule_id>` — confirm results still exist after rule deletion.

---

## Files to Create or Modify

### Backend
- **db_adapter.py**: Add functions 1-10 (listed above). Enhance existing `get_datasets_for_project()` if needed.
- **rule_engine.py**: Add safe rendering function (optional; current implementation may be sufficient, verify during implementation).

### Frontend
- **dash_app.py**:
  - Enhance `page_rules()` function to include Configured Rules table and modals.
  - Add Callbacks 1-10 (listed above).
  - Ensure global-toast is available for success/error messages (already exists at line ~30).

### Testing
- **test_rule_library.py**: New test file with 10+ unit and integration tests.

---

## Success Criteria

1. ✓ Project dropdown loads all active projects from DB (no hardcoding).
2. ✓ Dataset dropdown filters by selected project (dynamic).
3. ✓ Rule templates load from dq_rule_templates table (all 14 templates visible).
4. ✓ Clicking a template tile populates the configuration form with correct defaults.
5. ✓ SQL preview generates correct SQL using actual catalog.schema.table paths and template placeholders.
6. ✓ Save Rule validates all inputs and persists to dq_rules with audit logging.
7. ✓ Configured Rules table shows project/dataset-specific rules and refreshes after save.
8. ✓ Edit/Delete/Toggle operations work correctly and log to audit table.
9. ✓ Deleting/deactivating a rule preserves dq_results history (immutable).
10. ✓ SQL template rendering is injection-safe (validated identifiers, no raw interpolation).
11. ✓ No errors on any user action; all data from metadata tables (zero hardcoding).
12. ✓ Multiple projects can be managed independently on the same page.

---

## Implementation Order

1. **Backend (db_adapter.py)**: Implement functions 1-10. Verify each with quick SQL queries.
2. **Safe Rendering (rule_engine.py)**: Add or enhance rendering function if needed.
3. **HTML Layout (dash_app.py)**: Verify page_rules() has all required HTML structure. Add missing modals/stores.
4. **Callbacks (dash_app.py)**: Implement Callbacks 1-10 in sequence. Test each one individually.
5. **Integration Testing**: Run end-to-end flow tests.
6. **Unit Tests (test_rule_library.py)**: Write and run all tests. Ensure 100% pass rate.

---

## Notes

- **No new dependencies**: All functions use existing libraries (pandas, sqlite3, datetime).
- **Dual-mode architecture preserved**: All DB functions work with both Databricks and SQLite (via `get_db_mode()` and `query_df()` / `execute_cmd()`).
- **Audit logging mandatory**: Every mutation (INSERT, UPDATE, DELETE) must log to `dq_config_audit` with timestamp, user, old/new values.
- **History immutable**: Execution results in `dq_results` are never modified or deleted, even if the rule is deleted or deactivated.
- **Threshold semantics**: Different templates interpret threshold differently (e.g., NULL_COUNT: threshold = max allowed nulls; NULL_PERCENT: threshold = max allowed null %; DATA_FRESHNESS: threshold = max age in hours). The UI should provide context.
- **Column parameter logic**: Some templates require a column (NULL_COUNT, UNIQUE_COUNT, etc.). Others don't (ROW_COUNT, SCHEMA_CHANGE). The UI hides/shows column input based on `requires_column` from the template.
- **Target table logic**: Some templates require both source and target (REFERENTIAL_INTEGRITY, SOURCE_TARGET_ROW_COUNT). The UI should show a target table selector for these.


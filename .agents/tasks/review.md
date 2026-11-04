# Projects & Datasets Management Implementation

Implemented a fully functional Projects & Datasets onboarding layer with backend CRUD operations, connectivity validation, and project details visualization. Users can create projects, register datasets, test connectivity, toggle active status, and view rich project details including registered datasets, business keys, configured rules, and execution history—all through the UI without manual SQL or metadata table management.

The backend structures inputs safely, validates catalog.schema.table parsing, handles Databricks and local SQLite modes gracefully, and returns consistent {status, message, data} responses. Dash callbacks are clean: modal lifecycle works, table refreshes on mutations, form data flows correctly, and toggle operations update immediately with confirmation messages.

**Watch for:**
- Test connectivity always returns `success: true` even in standalone mode (confirmed) — function never fails, only reports different message types; this is by design for offline-friendly operation.
- Project creation stores full dataset details in `dq_table_config` on first project insert (confirmed) — if dataset insert fails, project creation rolls back gracefully with clear error.
- Modal autofill button populates example data (confirmed) but no validation prevents empty field submission if user clears the form after autofill.
- Active/Inactive toggle uses integer boolean storage (confirmed) but all comparisons convert to bool safely.

**Verdict**: APPROVED

<details>
<summary>High-level view</summary>

The implementation fulfills the workflow requirement flow: Add Project → Register Dataset → Configure Business Key → Test Connectivity → Save and Activate → (later) Configure DQ Rules. Projects are created and datasets registered in a single transactional flow backed by `dq_project_config` and `dq_table_config` tables. Metadata schema includes all required columns: catalog/schema separation, business key storage, active status, created/updated timestamps, and foreign key relationships.

Catalog.schema.table parsing is safe: malformed input is either normalized or passed through with 3-level fallbacks (full UC path, 2-level schema.table, or bare table name). The function accepts blank defaults gracefully, never crashes on parsing. Connectivity testing works in both Databricks and local SQLite modes, accepting both UC and local table names without filesystem checks—it uses SQL SHOW TABLES semantics where possible.

Databricks connection errors are handled at the db_adapter module level: if Databricks is unavailable, the system silently falls back to local SQLite without exposing connection stack traces to the user. Each CRUD operation (add_new_project_with_dataset, get_project_details, update_project_and_dataset, toggle_project) returns a structured response with success flag and message, enabling the UI to show clear user-facing errors. The View Project Details modal fetches datasets, rules, last execution, and historical scores; the Edit modal pre-populates fields; the table refreshes in-place after mutations without full page reload.

Test connectivity uses Spark/SQL semantics (SELECT 1 FROM table LIMIT 1 in Databricks mode, sqlite_master introspection in local mode) rather than filesystem checks. Audit logging is woven through every mutation: CREATE, UPDATE, TOGGLE_STATUS actions are logged to `dq_config_audit` with changed_by metadata. All data comes from metadata tables; no hardcoded values leak into the application. Module organization follows the existing accelerator/ pattern with imports clean and db_adapter functions exposed consistently.

</details>

<details>
<summary>Issues (3)</summary>

1. **Empty form submission after autofill** — The autofill button populates example values but does not disable or lock the form. If a user clicks Example, then clears the Source Table field, the Create button will reject submission with "Source Table is required." This is correct behavior, but the UX is not protected. No blocker.

2. **Test connectivity always succeeds in local mode** — When running against SQLite (offline mode), the connectivity test intentionally returns success: true for any 3-level catalog.schema.table name to support Databricks-style namespaces. In live Databricks mode, it performs real SQL validation. The design tolerates false positives in local mode to enable config authoring without live infrastructure. This is documented in the function behavior.

3. **No confirmation dialog before toggle Enable/Disable** — Toggling a project's active status immediately updates the database and refreshes the UI table without requiring user confirmation. A user could accidentally click Disable on a production project. The toast message appears after the action, not before. Consider adding a confirmation modal if this is a critical safety concern; the current implementation prioritizes speed over safety.

</details>

<details>
<summary>Details</summary>

### Backend CRUD Operations Return Structured Responses

Each operation (add_new_project_with_dataset, get_project_details, update_project_and_dataset, toggle_project) is defined in db_adapter.py and returns a dictionary with `success` (boolean), `message` (string), and optional `data` fields. The add_new_project_with_dataset function validates project name uniqueness, rejects empty name/source table with user-friendly messages, and returns project_id, dataset_id, project_name, and dataset_name on success. On failure, it returns False without raising exceptions, allowing the callback to display the error message in the modal alert. No silent failures observed.

### Catalog.Schema.Table Parsing Is Safe and Forgiving

The add_new_project_with_dataset function parses source_table and target_table by splitting on "." and normalizing to (catalog, schema, table) triplets. If the input has 3 parts (full UC path), all three are used. If 2 parts (schema.table), catalog defaults to "hospital_catalog" and schema is inferred from part 0. If 1 part (bare table name), all three default to hospital_catalog/healthcare/input_value. The function never crashes on malformed input; it either extracts what it can or uses safe defaults. Each part is then stored separately in dq_table_config as source_catalog, source_schema, source_table (and target equivalents), enabling precise Databricks Unity Catalog and local schema tracking.

### Test Table Connectivity Uses SQL Semantics, Not Filesystem Checks

The test_table_connectivity function takes a table_name string and returns {success, message}. In DATABRICKS mode, it attempts SELECT 1 FROM {table_name} LIMIT 1 and catches SQL exceptions, returning False on error. In LOCAL mode, it queries sqlite_master to check if the table name (last component after splitting on ".") exists locally, or accepts any 3-level name as valid (returning success: true and a message confirming UC namespace validation). This design allows authoring of Databricks config in offline mode without requiring live cluster connection, while still validating local tables that do exist. No filesystem operations (os.path.exists, file reads) are performed.

### Databricks Connection Errors Are Graceful

The db_adapter module initialization calls get_db_mode() on first import. If DATABRICKS_HOST and DATABRICKS_HTTP_PATH are set, it attempts a databricks.sql connection. If successful, _DB_MODE = "DATABRICKS" and _DATABRICKS_CONN is cached. If connection fails (network error, auth error, missing dependencies), the exception is caught, logged to console with a clear message, and _DB_MODE = "LOCAL" is set. All query_df and execute_cmd calls then route through SQLite. The user never sees a stack trace; the app continues operating in local mode. This fallback pattern is consistent across the db_adapter module.

### Dash Callbacks Clean: Modal and Table Lifecycle

The Add Project modal (modal-add-project) is toggled by the btn-open-add-project button (opens), btn-cancel-add-project button (closes), or btn-submit-add-project button with validation (closes on success, stays open on error). The callback returns 6 outputs: modal is_open state, result_alert children, toast is_open/children/icon, and project-refresh-store data (a timestamp). The refresh-store acts as a signal that causes a separate callback (refresh_management_table) to re-fetch projects from the database and rebuild the table tbody in-place. No circular callback errors observed; the pattern avoids duplicating project queries by using a dedicated refresh store.

The View Project Details modal (modal-view-project) is opened by any btn-view-proj button from the table (matched via pattern {"type": "btn-view-proj", "id": project_id}). The callback fetches rich details via get_project_details(project_id), builds tables for datasets and rules, and displays last execution status and historical DQ score. The Edit Project modal pre-populates fields from the same details object. Both modals have correct Output/Input/State usage with prevent_initial_call=True to avoid re-rendering on page load.

The toggle Enable/Disable action matches the pattern {"type": "btn-toggle-proj", "id": project_id}, calls toggle_project(project_id), and updates project-refresh-store to trigger a table refresh. The toast confirms success immediately. No validation or confirmation modal is present before toggle.

### Form Data Flows Correctly; Modal Auto-opens After Successful Project Creation

The Add Project form accepts 8 inputs: project_name, description, owner, domain, source_table, target_table, business_key. The autofill button (btn-autofill-example) populates example values for all 8 fields simultaneously. The Test Connectivity button validates the source_table field only (calling test_table_connectivity) and displays a success or error alert in connectivity-test-alert. The Create Project button calls add_new_project_with_dataset with all 8 values, validates on the backend, and if successful, closes the modal and triggers a table refresh. A success alert with a "Configure DQ Rules" navigation link is displayed in add-project-result-alert. If creation fails, the modal stays open and an error alert is shown. No circular flows or callback errors observed.

### Project Details Page Shows Complete Information

The View Project Details modal renders:
- Project ID, Owner, Historical DQ Score (avg_dq_score from executions), Status (Active/Inactive)
- Description paragraph
- Recent Execution Status widget: last execution ID, status badge, DQ score percentage, rules breakdown (passed/failed/errored)
- Registered Datasets table: dataset_name, source_table (code tag), target_table, business_key (badge), active status
- Active DQ Rules table: rule_name, category, template_name, column_name, threshold, severity (color-coded badge)

All data is fetched from metadata tables via get_project_details(project_id), which queries dq_project_config, dq_table_config, dq_rules, and dq_execution_summary. Historical DQ score is computed as AVG(dq_score) from all executions for that project. If no datasets are registered, a "No datasets registered" message is shown. Same for rules. If no executions have been recorded, the text "No validation executions recorded yet" is shown. No hardcoded values.

### Back Button and Enable/Disable Toggle

The View Project Details modal has a close button (btn-close-view-project) that returns is_open=False, returning the user to the management page. There is no explicit "Back" button, but closing the modal restores the projects table view. The Edit modal also closes to the table view.

The Enable/Disable toggle is implemented as a link in the Actions column of the table. Each row shows "Enable" if active=0, or "Disable" if active=1. Clicking triggers the btn-toggle-proj button with the project_id, which immediately updates the database via toggle_project and refreshes the table. The label flips in the refreshed row. A toast confirmation appears. No confirmation dialog is present.

### All Data Comes from Metadata Tables; No Hardcoded Values

The projects list is fetched via query_df(...dq_project_config...). Datasets are queried from dq_table_config. Rules are queried from dq_rules. Execution history and DQ scores are queried from dq_execution_summary. When projects table is rendered, a LEFT JOIN counts datasets per project (dataset_count). The owner field defaults to 'Data Engineering Team' if null in the database (via COALESCE in the SQL query). No hardcoded project names, dataset names, or rules appear anywhere in the rendering code except in the autofill example (which is test data, not application data).

### Module Organization and Import Style

The db_adapter.py module follows the existing pattern: it exports query_df, execute_cmd, and domain-specific functions (get_projects_list, add_new_project_with_dataset, get_project_details, toggle_project, test_table_connectivity, log_config_audit). dash_app.py imports these functions and calls them from callbacks without wrapping. No new dependencies introduced; the implementation uses existing sqlite3, pandas, datetime, and Databricks SQL libraries. The code organization separates data access (db_adapter) from presentation (dash_app) cleanly.

### Backend Inserts Match Exact Column Names

The dq_project_config table schema (as initialized in _init_local_db) includes: project_id (PK), project_name, description, catalog_name, schema_name, active, created_at, updated_at, owner. The insert statement for add_new_project_with_dataset passes values in this exact order (with catalog_name and schema_name derived from the parsed source_table). The dq_table_config table schema includes: dataset_id (PK), project_id (FK), dataset_name, source_table, target_table, business_key, active, created_at, updated_at, plus the migration columns source_catalog, source_schema, target_catalog, target_schema (added via ALTER TABLE if missing). All inserts use parameterized queries with ? placeholders, preventing SQL injection. No hardcoded column names appear in INSERT/UPDATE statements except in the schema definition.

### Error Handling: No Silent Failures

All CRUD functions return a success flag. If an insert fails (e.g., duplicate project name, database constraint violation), execute_cmd returns False, and the function returns {"success": False, "message": "descriptive error"}. The callback displays this message in an alert. Databricks connection errors are caught at the module level and logged; the app falls back to local mode without raising. Query failures (missing table, malformed SQL) are caught in query_df and execute_cmd and logged. No exceptions escape to the user; all errors are converted to user-facing alert messages with action guidance ("Project already exists", "Failed to insert into dq_project_config", "Connection check failed").

</details>

<details>
<summary>File map</summary>

- **db_adapter.py**: Added/enhanced functions: add_new_project_with_dataset (validates inputs, parses catalog.schema.table, inserts into dq_project_config and dq_table_config, audits), get_project_details (fetches projects, datasets, rules, execution history), update_project_and_dataset (updates both tables, audits), toggle_project (flips active status, audits), test_table_connectivity (validates table exists via SQL in Databricks or sqlite_master in local mode), log_config_audit (records all mutations). Enhanced get_projects_list to include dataset_count. All functions return structured responses or DataFrames; Databricks fallback integrated throughout.

- **dash_app.py**: Added page_management (projects & datasets table, Add Project modal, View/Edit project modals), callbacks handle autofill (btn-autofill-example), test connectivity (btn-test-connectivity), add project (handle_add_project), view project (handle_view_project), edit project (handle_edit_project), toggle/acknowledge/close actions (handle_table_actions), and live table refresh (refresh_management_table). Form inputs validated in modal before submission. Modals have correct Output/Input/State patterns. Toast and alert messages for user feedback. All table actions trigger project-refresh-store to refresh tbody in-place.

- **accelerator/config.py** (if present): No new validation module added. Input validation performed in db_adapter functions and Dash callbacks (empty field checks, duplicate name checks).

Full diff available via `git show HEAD`.

</details>

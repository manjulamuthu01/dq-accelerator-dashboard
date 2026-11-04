#!/usr/bin/env python3
"""Manual test script for Rule Library backend functions."""

import sys
sys.path.insert(0, '.')

import db_adapter

print("=" * 80)
print("TESTING RULE LIBRARY BACKEND FUNCTIONS")
print("=" * 80)

# Test 1: Get Projects for Dropdown
print("\n1. Testing get_projects_for_dropdown()...")
try:
    projects_df = db_adapter.get_projects_for_dropdown()
    print(f"   OK Found {len(projects_df)} projects:")
    for _, p in projects_df.iterrows():
        print(f"     - {p['project_name']} (ID: {p['project_id']})")
except Exception as e:
    print(f"   ERROR: {e}")

# Test 2: Get Datasets for Project
print("\n2. Testing get_datasets_for_project('Hospital')...")
try:
    datasets = db_adapter.get_datasets_for_project("Hospital")
    print(f"   OK Found {len(datasets)} datasets: {datasets}")
except Exception as e:
    print(f"   ERROR: {e}")

# Test 3: Get Rule Templates
print("\n3. Testing get_rule_templates()...")
try:
    templates_df = db_adapter.get_rule_templates()
    print(f"   OK Found {len(templates_df)} templates:")
    for _, t in templates_df.iterrows():
        print(f"     - {t['template_name']} ({t['category']})")
except Exception as e:
    print(f"   ERROR: {e}")

# Test 4: Get Configured Rules for Hospital/source_table
print("\n4. Testing get_configured_rules(project_id=1, dataset_id=1)...")
try:
    rules_df = db_adapter.get_configured_rules(1, 1)
    print(f"   OK Found {len(rules_df)} configured rules for Hospital/source_table:")
    for _, r in rules_df.iterrows():
        print(f"     - {r['rule_name']} on {r['column_name']} (threshold: {r['threshold']}, severity: {r['severity']})")
except Exception as e:
    print(f"   ERROR: {e}")

# Test 5: Generate SQL Preview
print("\n5. Testing generate_sql_preview(project_id=1, dataset_id=1, template_name='NULL_COUNT', column_name='patient_name')...")
try:
    sql = db_adapter.generate_sql_preview(1, 1, "NULL_COUNT", "patient_name")
    print(f"   OK Generated SQL:")
    for line in sql.split('\n'):
        print(f"     {line}")
except Exception as e:
    print(f"   ERROR: {e}")

# Test 6: Save a New Rule
print("\n6. Testing save_rule()...")
saved_rule_id = None
try:
    # First check if project 1 exists and activate it if needed
    check_proj = db_adapter.query_df("SELECT * FROM dq_project_config WHERE project_id = 1")
    print(f"   Debug: Project 1 check - {len(check_proj)} rows")
    if not check_proj.empty:
        print(f"   Debug: Project 1 found: active = {check_proj.iloc[0]['active']}")
        if not check_proj.iloc[0]['active']:
            # Activate project 1
            db_adapter.execute_cmd("UPDATE dq_project_config SET active = 1 WHERE project_id = 1")
            print(f"   Debug: Activated Project 1")
    
    result = db_adapter.save_rule(
        project_id=1,
        dataset_id=1,
        template_name="NULL_COUNT",
        rule_name="TEST_NULL_COUNT",
        column_name="test_column",
        threshold=0,
        severity="High",
        active=True,
        user="test_admin"
    )
    print(f"   OK save_rule result: {result}")
    if result["success"]:
        saved_rule_id = result["rule_id"]
        print(f"     New rule_id: {saved_rule_id}")
    else:
        print(f"     Error: {result['message']}")
except Exception as e:
    print(f"   ERROR: {e}")
    import traceback
    traceback.print_exc()
    saved_rule_id = None
    saved_rule_id = None

# Test 7: Get Rule Details
if saved_rule_id:
    print(f"\n7. Testing get_rule_details({saved_rule_id})...")
    try:
        rule_data = db_adapter.get_rule_details(saved_rule_id)
        print(f"   OK Rule details:")
        for key, value in rule_data.items():
            print(f"     {key}: {value}")
    except Exception as e:
        print(f"   ERROR: {e}")

# Test 8: Update Rule
if saved_rule_id:
    print(f"\n8. Testing update_rule({saved_rule_id})...")
    try:
        result = db_adapter.update_rule(
            rule_id=saved_rule_id,
            rule_name="TEST_NULL_COUNT_UPDATED",
            column_name="test_column_updated",
            threshold=5,
            severity="Medium",
            active=False,
            user="test_admin"
        )
        print(f"   OK Update result: {result}")
    except Exception as e:
        print(f"   ERROR: {e}")

# Test 9: Toggle Rule Active Status
if saved_rule_id:
    print(f"\n9. Testing toggle_rule_active({saved_rule_id})...")
    try:
        result = db_adapter.toggle_rule_active(saved_rule_id, user="test_admin")
        print(f"   OK Toggle result: {result}")
    except Exception as e:
        print(f"   ERROR: {e}")

# Test 10: Delete Rule
if saved_rule_id:
    print(f"\n10. Testing delete_rule({saved_rule_id}, hard_delete=False)...")
    try:
        result = db_adapter.delete_rule(saved_rule_id, user="test_admin", hard_delete=False)
        print(f"   OK Delete result: {result}")
    except Exception as e:
        print(f"   ERROR: {e}")

print("\n" + "=" * 80)
print("TESTING COMPLETE")
print("=" * 80)

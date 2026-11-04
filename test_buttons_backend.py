#!/usr/bin/env python
"""Test backend functions that buttons call"""

import sys
import db_adapter
from datetime import datetime

# Fix Unicode encoding
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

print("=" * 70)
print("TESTING BUTTON BACKEND FUNCTIONS")
print("=" * 70)

# 1. Get a project to test View/Edit
print("\n[1] Fetching first project from database...")
projects = db_adapter.get_projects_list()
if len(projects) > 0:
    p_id = projects.iloc[0]['project_id']
    print(f"[OK] Found project ID: {p_id} ({projects.iloc[0]['project_name']})")
    
    # 2. Test View (get_project_details)
    print(f"\n[2] Testing View button (get_project_details({p_id}))...")
    try:
        details = db_adapter.get_project_details(p_id)
        print(f"  Details type: {type(details)}")
        print(f"  Details keys: {details.keys() if isinstance(details, dict) else 'Not a dict'}")
        if details and 'project' in details:
            print(f"[OK] get_project_details works")
            print(f"  Project: {details['project']['project_name']}")
            print(f"  Datasets: {len(details['datasets'])}")
            print(f"  Rules: {len(details['rules'])}")
        else:
            print(f"[FAIL] get_project_details returned unexpected format")
    except Exception as e:
        print(f"[FAIL] get_project_details failed: {e}")
        import traceback
        traceback.print_exc()
    
    # 3. Test Edit (update_project_and_dataset)
    print(f"\n[3] Testing Edit button (update_project_and_dataset)...")
    try:
        db_adapter.update_project_and_dataset(
            project_id=p_id,
            project_name=projects.iloc[0]['project_name'],
            description="Test edit",
            owner="Test Owner",
            dataset_id=None,
            source_table="test.schema.table",
            target_table="test.schema.target",
            business_key="test_id",
            user="admin"
        )
        print(f"[OK] update_project_and_dataset works")
    except Exception as e:
        print(f"[FAIL] update_project_and_dataset failed: {e}")
        import traceback
        traceback.print_exc()
    
    # 4. Test Enable/Disable (toggle_project)
    print(f"\n[4] Testing Enable/Disable button (toggle_project)...")
    try:
        before = db_adapter.get_project_details(p_id)
        if before and 'project' in before:
            before_status = before['project']['active']
            print(f"  Before toggle: active={before_status}")
            
            db_adapter.toggle_project(p_id)
            print(f"[OK] toggle_project executed")
            
            after = db_adapter.get_project_details(p_id)
            if after and 'project' in after:
                after_status = after['project']['active']
                print(f"  After toggle: active={after_status}")
                print(f"  Status changed: {before_status != after_status}")
            else:
                print(f"[FAIL] Could not get project details after toggle")
        else:
            print(f"[FAIL] Could not get project details before toggle")
    except Exception as e:
        print(f"[FAIL] toggle_project failed: {e}")
        import traceback
        traceback.print_exc()
else:
    print("[FAIL] No projects found in database")

print("\n" + "=" * 70)
print("BACKEND TEST COMPLETE")
print("=" * 70)

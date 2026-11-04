import db_adapter

print("Checking projects in database...")
projects = db_adapter.get_projects_list()
print(f"\nTotal projects: {len(projects)}")
print(f"Columns: {projects.columns.tolist()}")

for i, row in projects.iterrows():
    p_id = row['project_id']
    p_name = row['project_name']
    p_id_type = type(p_id).__name__
    print(f"\nProject {i}:")
    print(f"  ID: {p_id} (type: {p_id_type})")
    print(f"  Name: {p_name}")
    
    # Try to get details with the raw ID
    print(f"  Testing get_project_details({p_id})...")
    details = db_adapter.get_project_details(p_id)
    print(f"    Result: {details}")
    
    # Try with int conversion
    print(f"  Testing get_project_details(int({p_id}))...")
    details2 = db_adapter.get_project_details(int(p_id))
    print(f"    Result: {details2}")

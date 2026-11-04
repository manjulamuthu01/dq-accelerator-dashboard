from setuptools import setup, find_packages

setup(
    name="databricks-dq-accelerator",
    version="1.0.0",
    description="Dynamic Data Quality Accelerator for Databricks Lakehouse",
    author="Data Engineering & Quality Team",
    packages=find_packages(),
    install_requires=[
        "dash>=2.17",
        "dash-bootstrap-components>=1.6",
        "plotly>=5.20",
        "pandas>=2.0",
        "databricks-sql-connector>=3.4",
        "databricks-sdk>=0.32",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
)


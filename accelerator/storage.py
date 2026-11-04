"""
Storage and Persistence Adapter Module (Phase 8)
Supports Databricks SQL Warehouse (Unity Catalog Delta) and Local SQLite engine.
"""

import os
import sqlite3
import pandas as pd
from typing import Optional, Dict, Any, List
from accelerator.config import Config

class StorageAdapter:
    def __init__(self, db_file: Optional[str] = None):
        self.db_file = db_file or os.path.join(os.path.dirname(os.path.dirname(__file__)), "dq_accelerator.db")
        self._mode = "UNCHECKED"
        self._databricks_conn = None

    def get_mode(self) -> str:
        if self._mode != "UNCHECKED":
            return self._mode
        if Config.DATABRICKS_HOST and Config.DATABRICKS_HTTP_PATH:
            try:
                from databricks import sql
                hostname = Config.DATABRICKS_HOST.replace("https://", "").replace("http://", "").split("/")[0]
                auth_args = {"server_hostname": hostname, "http_path": Config.DATABRICKS_HTTP_PATH}
                if Config.DATABRICKS_TOKEN:
                    auth_args["access_token"] = Config.DATABRICKS_TOKEN
                else:
                    try:
                        from databricks.sdk.core import Config as SDKConfig
                        _cfg = SDKConfig()
                        auth_args["credentials_provider"] = lambda: _cfg.authenticate
                    except Exception:
                        pass
                self._databricks_conn = sql.connect(**auth_args)
                self._mode = "DATABRICKS"
                return self._mode
            except Exception:
                pass
        self._mode = "LOCAL"
        return self._mode

    def query(self, sql_query: str, params: tuple = ()) -> pd.DataFrame:
        if self.get_mode() == "DATABRICKS" and self._databricks_conn:
            try:
                with self._databricks_conn.cursor() as cursor:
                    cursor.execute(sql_query)
                    return cursor.fetchall_arrow().to_pandas()
            except Exception:
                pass
        
        conn = sqlite3.connect(self.db_file)
        try:
            return pd.read_sql_query(sql_query, conn, params=params)
        finally:
            conn.close()

    def execute(self, sql_statement: str, params: tuple = ()) -> bool:
        if self.get_mode() == "DATABRICKS" and self._databricks_conn:
            try:
                with self._databricks_conn.cursor() as cursor:
                    cursor.execute(sql_statement)
                    return True
            except Exception:
                pass
        
        conn = sqlite3.connect(self.db_file)
        try:
            cur = conn.cursor()
            cur.execute(sql_statement, params)
            conn.commit()
            return True
        except Exception:
            return False
        finally:
            conn.close()


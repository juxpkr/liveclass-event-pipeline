import pandas as pd
import streamlit as st
from deltalake import DeltaTable

from src.config import STORAGE_OPTIONS

@st.cache_data(ttl=5)
def load_delta_table(path: str) -> pd.DataFrame:
    try:
        delta_table = DeltaTable(path, storage_options=STORAGE_OPTIONS)
        return delta_table.to_pandas()
    except Exception as error:
        st.error(f"Delta table load failed: {path}\n\n{error}")
        return pd.DataFrame()
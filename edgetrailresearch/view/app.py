import streamlit as st 
from edgetrailresearch.view.elements import *
import plotly.express as px

st.title("Macro Monitor") 

tab1, tab2, tab3 = st.tabs(["Growth", "Inflation","Liquidity"])

with tab1: 
    tab1, tab2, tab3 = st.tabs(["Demand Side", "Supply Side", "Macro Accounts"])
    with tab1:
        col1,col2 = st.columns(2)
        with col1:
            rgdp_table()
        with col2: 
            pass
            





with tab2:
    pass
with tab3:
    pass
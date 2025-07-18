from edgetrailresearch.models.prices import *
from edgetrailresearch.data.storage.data_store import *

import streamlit as st
import datetime as dt
from dotenv import load_dotenv 

load_dotenv()

#init list of dataframes
dfs = []

for i in YF_TICKERS: 
    data  = load_data(i)
    df = data.iloc[:,1]
    print(df) 



# def rgdp_table():
#     rgdp_tickers = ["GDPC1", "PCECC96", "GPDIC1", "NETEXC", "GCEC1" ] 
#     dfs = []
#     for i in rgdp_tickers:
#         df = fred_pull(i) 
#         dfs.append(df) 
        
#     df_merged = pd.concat([df.set_index("Date") for df in dfs], axis=1) 
#     df_merged.index = df_merged.index.strftime('%Y-%m-%d')
#     final_df = (df_merged.pct_change()*100).round(2)
#     final_df = final_df.rename(
#         columns=
#         {"GDPC1_value": "RGDP",
#         "PCECC96_value": "Consumption",
#         "GPDIC1_value": "Investment",
#         "NETEXC_value": "Net Exports",
#         "GCEC1_value": "Government"
#         }).tail().T
    
#     styled_df = (
#         final_df.style
#         .background_gradient(subset=final_df.columns[0], cmap = "coolwarm") 
#         .background_gradient(subset=final_df.columns[1], cmap = "coolwarm")
#         .background_gradient(subset=final_df.columns[2], cmap = "coolwarm") 
#         .background_gradient(subset=final_df.columns[3], cmap = "coolwarm")
#         .background_gradient(subset=final_df.columns[4], cmap = "coolwarm")
#         .format({
#             final_df.columns[0]: "{:.2f}",
#             final_df.columns[1]: "{:.2f}",
#             final_df.columns[2]: "{:.2f}",
#             final_df.columns[3]: "{:.2f}",
#             final_df.columns[4]: "{:.2f}"
#         })
#     )

#     st.dataframe(styled_df) 


# def rgdp_plot():
#     pass 
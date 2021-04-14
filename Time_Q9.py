import streamlit as st
import pandas as pd 
import matplotlib.pyplot as plt
import ibm_db_dbi
import ibm_db as db
import plotly.graph_objects as go
from plotly import tools
import plotly.offline as py
import plotly.express as px
import datetime
from PIL import Image

st.set_option('deprecation.showPyplotGlobalUse', False)

#fuction to connect
def get_sql():
    try:
        conn = ibm_db_dbi.connect('DATABASE=q9test1;'
                'HOSTNAME=mtelbrus.rchland.ibm.com;'  # 127.0.0.1 or localhost works if it's local
                'PORT=50000;'
                'PROTOCOL=TCPIP;'
                'UID=testeng;'
                'PWD=Data.Science4Test;', '', '')
    except:
        print("No connection:")
    else:
        print("the Connection was successful")
    return conn
#Get info into Data Frame
@st.cache(allow_output_mutation=True)
def get_df(date_start, date_end):
    #-----------------------------------------------------
    getalldata= f"""WITH ESS(MFGN,ORNO,TESTLOGICAL,PROD) AS (
    select DISTINCT TCMFGN AS MFGN,TCORNO AS ORNO,TCMFS_TEST_CELL AS TESTLOGICAL,WUPROD AS PROD
    from testengq.tcogda
    join "QRYQ"."MFSGWU10_GRC" on tcmfgn = wumfgn and tcorno = WUWORN
    where  START_TIMESTAMP between '{date_start}' and '{date_end}' and TEST_RUN_TIME is not null  and TCMMDL in ('22E') and substring(TCORNO,1,1) != 'Y' and WUPROD = 'ZZ_EDATA')
    select distinct T.TCROUN,T.TCMFGN,T.TCORNO,T.TCMFS_TEST_CELL,T.TCCECC,T.TCPRLN,T.TCMATP,T.TCMMDL,T.START_TIMESTAMP,T.FINISH_TIMESTAMP,T.TOTAL_TIME,T.HOLD_MIN,T.HOLD_CNT, 
    case
    when T.TCPRLN in ('T_P8BRMP','T_P8BRZP') and TCCECC < 3 then 'Brazos 1-2 CECs AIX/Linux'
    when T.TCPRLN in ('T_P9ZZP','T_P9ZZL') and T.TCMMDL not in ('22G','41G','42G','22E') then 'ZZ AIX/Linux'
    when T.TCMMDL in ('22G','41G','42G','22E') and PROD IS NULL and T.TCPRLN in ('T_P9ZZP','T_P9ZZL') then 'ZZ Gen4 AIX/Linux'
    when T.TCMMDL in ('22G','41G','42G','22E') and PROD IS NULL and T.TCPRLN in ('T_P9ZZI','T_P9ZZM') then 'ZZ Gen4 IBM i'
    when T.TCMMDL in ('22E') and PROD = 'ZZ_EDATA' then 'ZZ Gen 4 ESS 5K'
    when T.TCPRLN in ('T_P9ZEP')  then 'Zeppelin'
    when T.TCPRLN in ('T_P8TULP')  then 'Tuleta AIX/Linux'
    when T.TCPRLN in ('T_P8TULI')  then 'Tuleta / IBM i'
    when T.TCPRLN in ('T_OPWSPN')  then 'Witherspoon'
    when T.TCPRLN in ('T_OPMHWK')  then 'Mihawk'
    when T.TCPRLN in ('T_OPGRSN')  then 'Garrison'
    when T.TCPRLN in ('T_OPFIRE')  then 'Firestone'
    when T.TCPRLN in ('T_P9ZZI','T_P9ZZM')  then 'ZZ IBM i'
    when T.TCPRLN in('T_P8BRMI','T_P8BRZI') and T.TCCECC < 3 then 'Brazos 1-2 CECs IBM i'
    when T.TCPRLN in ('T_P8BRMI','T_P8BRZI','T_P8BRMP','T_P8BRZP') and T.TCCECC > 2 then 'Brazos 3-4 CECs'
    when T.TCPRLN in ('T_P9FLMP','T_P9FLTP','T_P9FIPR') and T.TCCECC < 3 then 'Fleetwood AIX/Linux'
    when T.TCPRLN in ('T_P9FLMI','T_P9FLTI') and T.TCCECC < 3 then 'Fleetwood IBM i'
    when T.TCPRLN in ('T_P9FLMI','T_P9FLMP','T_P9FLTI','T_P9FLTP','T_P9FIPR') and T.TCCECC > 2 then 'Fleetwood 3-4 CECs'
    when T.TCPRLN in ('T_P8ALPQ','T_P8ALP ')  then 'Alpine'
    end as NEWPRLN
    from testengq.tcogda as T LEFT JOIN ESS 
    ON TCMFGN = MFGN AND TCORNO = ORNO AND TCMFS_TEST_CELL = TESTLOGICAL
    where T.START_TIMESTAMP between '{date_start}' and '{date_end}' and T.TEST_RUN_TIME is not null and substring(T.TCORNO,1,1) != 'Y' AND T.TCROUN = 1"""
    get_missed_rows = f"""WITH ESS(MFGN,ORNO,TESTLOGICAL,PROD) AS (
    select DISTINCT TCMFGN AS MFGN,TCORNO AS ORNO,TCMFS_TEST_CELL AS TESTLOGICAL,WUPROD AS PROD
    from testengq.tcogda
    join "QRYQ"."MFSGWU10_GRC" on tcmfgn = wumfgn and tcorno = WUWORN
    where  START_TIMESTAMP between '{date_start}' and '{date_end}' and TEST_RUN_TIME is not null  and TCMMDL in ('22E') and substring(TCORNO,1,1) != 'Y' and WUPROD = 'ZZ_EDATA')
    select distinct T.TCMFS_TEST_CELL
    from testengq.tcogda as T LEFT JOIN ESS 
    ON TCMFGN = MFGN AND TCORNO = ORNO AND TCMFS_TEST_CELL = TESTLOGICAL
    where T.START_TIMESTAMP between '{date_start}' and '{date_end}' and T.TEST_RUN_TIME is not null and substring(T.TCORNO,1,1) != 'Y' --AND T.TCROUN = 1  
    EXCEPT
    --Detalle order load
    select distinct TA.TCMFS_TEST_CELL
    from testengq.tcogda as TA LEFT JOIN ESS 
    ON TCMFGN = MFGN AND TCORNO = ORNO AND TCMFS_TEST_CELL = TESTLOGICAL
    where TA.START_TIMESTAMP between '{date_start}' and '{date_end}' and TA.TEST_RUN_TIME is not null and substring(TA.TCORNO,1,1) != 'Y' AND TA.TCROUN = 1 """
    pivot_table= f"""WITH ESS(MFGN,ORNO,TESTLOGICAL,PROD) AS (
    select DISTINCT TCMFGN AS MFGN,TCORNO AS ORNO,TCMFS_TEST_CELL AS TESTLOGICAL,WUPROD AS PROD
    from testengq.tcogda
    join "QRYQ"."MFSGWU10_GRC" on tcmfgn = wumfgn and tcorno = WUWORN
    where  START_TIMESTAMP between '{date_start}' and '{date_end}' and TEST_RUN_TIME is not null  and TCMMDL in ('22E') and substring(TCORNO,1,1) != 'Y' and WUPROD = 'ZZ_EDATA')
    ,
    PRLN AS (
    select distinct T.TCROUN,T.TCMFGN,T.TCORNO,T.TCMFS_TEST_CELL,T.TCCECC,T.TCPRLN,T.TCMATP,T.TCMMDL,T.START_TIMESTAMP,T.FINISH_TIMESTAMP,T.TOTAL_TIME,T.HOLD_MIN,T.HOLD_CNT, 
    case
    when T.TCPRLN in ('T_P8BRMP','T_P8BRZP') and TCCECC < 3 then 'Brazos 1-2 CECs AIX/Linux'
    when T.TCPRLN in ('T_P9ZZP','T_P9ZZL') and T.TCMMDL not in ('22G','41G','42G','22E') then 'ZZ AIX/Linux'
    when T.TCMMDL in ('22G','41G','42G','22E') and PROD IS NULL and T.TCPRLN in ('T_P9ZZP','T_P9ZZL') then 'ZZ Gen4 AIX/Linux'
    when T.TCMMDL in ('22G','41G','42G','22E') and PROD IS NULL and T.TCPRLN in ('T_P9ZZI','T_P9ZZM') then 'ZZ Gen4 IBM i'
    when T.TCMMDL in ('22E') and PROD = 'ZZ_EDATA' then 'ZZ Gen 4 ESS 5K'
    when T.TCPRLN in ('T_P9ZEP')  then 'Zeppelin'
    when T.TCPRLN in ('T_P8TULP')  then 'Tuleta AIX/Linux'
    when T.TCPRLN in ('T_P8TULI')  then 'Tuleta / IBM i'
    when T.TCPRLN in ('T_OPWSPN')  then 'Witherspoon'
    when T.TCPRLN in ('T_OPMHWK')  then 'Mihawk'
    when T.TCPRLN in ('T_OPGRSN')  then 'Garrison'
    when T.TCPRLN in ('T_OPFIRE')  then 'Firestone'
    when T.TCPRLN in ('T_P9ZZI','T_P9ZZM')  then 'ZZ IBM i'
    when T.TCPRLN in('T_P8BRMI','T_P8BRZI') and T.TCCECC < 3 then 'Brazos 1-2 CECs IBM i'
    when T.TCPRLN in ('T_P8BRMI','T_P8BRZI','T_P8BRMP','T_P8BRZP') and T.TCCECC > 2 then 'Brazos 3-4 CECs'
    when T.TCPRLN in ('T_P9FLMP','T_P9FLTP','T_P9FIPR') and T.TCCECC < 3 then 'Fleetwood AIX/Linux'
    when T.TCPRLN in ('T_P9FLMI','T_P9FLTI') and T.TCCECC < 3 then 'Fleetwood IBM i'
    when T.TCPRLN in ('T_P9FLMI','T_P9FLMP','T_P9FLTI','T_P9FLTP','T_P9FIPR') and T.TCCECC > 2 then 'Fleetwood 3-4 CECs'
    when T.TCPRLN in ('T_P8ALPQ','T_P8ALP ')  then 'Alpine'
    end as NEWPRLN
    from testengq.tcogda as T LEFT JOIN ESS 
    ON TCMFGN = MFGN AND TCORNO = ORNO AND TCMFS_TEST_CELL = TESTLOGICAL
    where T.START_TIMESTAMP between '{date_start}' and '{date_end}' and T.TEST_RUN_TIME is not null and substring(T.TCORNO,1,1) != 'Y' AND T.TCROUN = 1 )
    SELECT NEWPRLN,count(*) as VOLUME,round(avg(TOTAL_TIME)) AS AVGERAGE
    FROM PRLN 
    GROUP BY NEWPRLN"""
    alltimes_by_product = f"""WITH ESS(MFGN,ORNO,TESTLOGICAL,PROD) AS (
    select DISTINCT TCMFGN AS MFGN,TCORNO AS ORNO,TCMFS_TEST_CELL AS TESTLOGICAL,WUPROD AS PROD
    from testengq.tcogda
    join "QRYQ"."MFSGWU10_GRC" on tcmfgn = wumfgn and tcorno = WUWORN
    where  START_TIMESTAMP between '{date_start}' and '{date_end}' and TEST_RUN_TIME is not null  and TCMMDL in ('22E') and substring(TCORNO,1,1) != 'Y' and WUPROD = 'ZZ_EDATA')
    select distinct *, 
    case
    when T.TCPRLN in ('T_P8BRMP','T_P8BRZP') and TCCECC < 3 then 'Brazos 1-2 CECs AIX/Linux'
    when T.TCPRLN in ('T_P9ZZP','T_P9ZZL') and T.TCMMDL not in ('22G','41G','42G','22E') then 'ZZ AIX/Linux'
    when T.TCMMDL in ('22G','41G','42G','22E') and PROD IS NULL and T.TCPRLN in ('T_P9ZZP','T_P9ZZL') then 'ZZ Gen4 AIX/Linux'
    when T.TCMMDL in ('22G','41G','42G','22E') and PROD IS NULL and T.TCPRLN in ('T_P9ZZI','T_P9ZZM') then 'ZZ Gen4 IBM i'
    when T.TCMMDL in ('22E') and PROD = 'ZZ_EDATA' then 'ZZ Gen 4 ESS 5K'
    when T.TCPRLN in ('T_P9ZEP')  then 'Zeppelin'
    when T.TCPRLN in ('T_P8TULP')  then 'Tuleta AIX/Linux'
    when T.TCPRLN in ('T_P8TULI')  then 'Tuleta / IBM i'
    when T.TCPRLN in ('T_OPWSPN')  then 'Witherspoon'
    when T.TCPRLN in ('T_OPMHWK')  then 'Mihawk'
    when T.TCPRLN in ('T_OPGRSN')  then 'Garrison'
    when T.TCPRLN in ('T_OPFIRE')  then 'Firestone'
    when T.TCPRLN in ('T_P9ZZI','T_P9ZZM')  then 'ZZ IBM i'
    when T.TCPRLN in('T_P8BRMI','T_P8BRZI') and T.TCCECC < 3 then 'Brazos 1-2 CECs IBM i'
    when T.TCPRLN in ('T_P8BRMI','T_P8BRZI','T_P8BRMP','T_P8BRZP') and T.TCCECC > 2 then 'Brazos 3-4 CECs'
    when T.TCPRLN in ('T_P9FLMP','T_P9FLTP','T_P9FIPR') and T.TCCECC < 3 then 'Fleetwood AIX/Linux'
    when T.TCPRLN in ('T_P9FLMI','T_P9FLTI') and T.TCCECC < 3 then 'Fleetwood IBM i'
    when T.TCPRLN in ('T_P9FLMI','T_P9FLMP','T_P9FLTI','T_P9FLTP','T_P9FIPR') and T.TCCECC > 2 then 'Fleetwood 3-4 CECs'
    when T.TCPRLN in ('T_P8ALPQ','T_P8ALP ')  then 'Alpine'
    end as NEWPRLN
    from testengq.tcogda as T LEFT JOIN ESS 
    ON TCMFGN = MFGN AND TCORNO = ORNO AND TCMFS_TEST_CELL = TESTLOGICAL
    where T.START_TIMESTAMP between '{date_start}' and '{date_end}' and T.TEST_RUN_TIME is not null and substring(T.TCORNO,1,1) != 'Y' AND T.TCROUN = 1 
    order by TOTAL_TIME DESC"""
    df = pd.read_sql(getalldata, get_sql())
    df1 = pd.read_sql(get_missed_rows, get_sql())
    df2 = pd.read_sql(pivot_table, get_sql())
    df3 = pd.read_sql(alltimes_by_product, get_sql())
    return df, df1, df2, df3;

#---------------------------------------
#Calculate Streamlit
#---------------------------------------
""" 
# Review times to execution in Power
this app can show you how many time spend every kind of maintenance
you can filter with the following parameters.

## Tecnologies
1. Python libraries: Pandas, Matplotlib, Streamlit, ibm_db_dbi
2. DB2
"""
#--------------------------------------------------------------
#body to side bar 
#--------------------------------------------------------------
l_column, r_column = st.beta_columns((1,1))
date_start = l_column.date_input("Choose the start date", datetime.date(2021, 2,1))
date_end = r_column.date_input("Choose the end date", datetime.date(2021, 3,1))
df_alldata, df_except_tl, df_pivot_table, df1 = get_df(date_start, date_end)
df_alldata.fillna(0, inplace=True)
df1.fillna(0, inplace=True)

df_gran1 = df_alldata[df_alldata["TCROUN"] > 1]
df_alldata = df_alldata[df_alldata["TCROUN"] == 1]
st.sidebar.write("""# Slider to filter by Product
**Refered to first table**""")
df_only_names = df_alldata['NEWPRLN'].unique()
filter_by_product = st.sidebar.multiselect("Select Product Name", df_only_names)
mask_products = df_alldata['NEWPRLN'].isin(filter_by_product)
by_products = df_alldata[mask_products]

st.sidebar.write("---")
list_brand = ["Fleetwood 3-4 CECs", "Fleetwood AIX/Linux", "Mihawk", "Witherspoon", "Zeppelin", "ZZ AIX/Linux", "ZZ Gen 4 ESS 5K", "ZZ Gen4 AIX/Linux", "ZZ Gen4 IBM i", "ZZ IBM i"]
st.sidebar.write("# Filter by Order Number")
brand_type = st.sidebar.selectbox("Can you choose brand?", ("Fleetwood 3-4 CECs", "Fleetwood AIX/Linux", "Mihawk", 
    "Witherspoon", "Zeppelin", "ZZ AIX/Linux", "ZZ Gen 4 ESS 5K", "ZZ Gen4 AIX/Linux", "ZZ Gen4 IBM i", "ZZ IBM i"))
if brand_type in list_brand:
    dict_target = {
        "Fleetwood 3-4 CECs" : "72", 
        "Fleetwood AIX/Linux" : "48",
        "Mihawk" : "12", 
        "Witherspoon" : "10",
        "Zeppelin": "30", 
        "ZZ AIX/Linux" : "20", 
        "ZZ Gen 4 ESS 5K" : "60",
        "ZZ Gen4 AIX/Linux" : "30",
        "ZZ Gen4 IBM i": "30",
        "ZZ IBM i": "30"
    }
    st.sidebar.write("**Brand** " +brand_type+" **the Target is** "  + dict_target.get(brand_type, brand_type ))

#--------------------------------------------------------------
# body with Streamlit
#---------------------------------------------------------------

#--------------------------------------------------------
#showing rows with 2 or more execution
if st.checkbox("show products with more 1 execution"):
    if df_gran1.empty:
        st.write("## Sorry, there aren't nothing to show")
        st.write("------------------------------------")
    else:
        st.write(df_gran1)
#-------------------------------
# Use the full page instead of a narrow central column
#showing DF filtered by multiselect of products
#-------------------------------------------------------
#DF by productos
st.write(by_products)
#chart
#fig = px.bar(graph_df, x=graph_df.index, y=graph_df.columns, height=800, width=1000)
#st.write(fig.show())
df = df1[df1["NEWPRLN"]== brand_type]
#dropna rows if has more of 1 execution
df_gran1 = df_alldata[df_alldata["TCROUN"] > 1]
df_alldata = df_alldata[df_alldata["TCROUN"] == 1]
#------------------ Calculate the bar chart by minutes -------------------
df['Test'] = df['TEST_RUN_TIME'].apply(lambda x: x/60)
df['Rework'] = df['TEST_RWK_MIN'] + df['SWPRE_RWK_MIN'] + df['EXT_RWK_MIN']
df['Rework'] = df['Rework'].apply(lambda x: x/60)
df['Preload'] = df['SWPRE_RUN_TIME'].apply(lambda x : x/60)
df['Attention'] = df['TEST_ATN_MIN'] + df['SWPRE_ATN_MIN'] + df['EXT_ATN_MIN']
df['Attention'] = df['Attention'].apply(lambda x : x/60)
df['EXT_OPS'] = df['EXT_RUN_TIME'].apply(lambda x :x/60)
df['Hold'] = df['HOLD_MIN'].apply(lambda x :x/60)
#-----------------Organize df -----------------------------
graph_df = df[['TCORNO','Test','Rework','Preload','Attention','EXT_OPS','Hold']].copy()
graph_df = graph_df.rename(columns={'TCORNO': 'ORDER_NUMBER'})
graph_df = graph_df.set_index('ORDER_NUMBER')
count = graph_df.groupby('ORDER_NUMBER').count()
target = count['Test'].values.copy()
target[:]= 10 # target about each product
fig = px.bar(graph_df, x=graph_df.index, y=graph_df.columns,height=600, width=850)
fig.update_layout(
    title = "<b> Ordered by Order Number <b>"
)
st.plotly_chart(fig)

left_col, right_col = st.beta_columns((2,1))
right_col.write("## Exceptions Orders")
right_col.write(df_except_tl)
st.write("---")
left_col.write("## Pivot table")
left_col.write(df_pivot_table)



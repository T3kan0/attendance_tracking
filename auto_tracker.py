#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 21 15:52:14 2025

@author: mbonanits
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from dateutil import parser
import re
from datetime import datetime
from sqlalchemy import create_engine
from rapidfuzz import process
from fuzzywuzzy import process
from rapidfuzz import process, fuzz
from supabase import create_client, Client
from datetime import date

st.set_page_config(layout="wide")


# Load credentials from Streamlit secrets

SUPABASE_URL1 = st.secrets["tutor"]["SUPABASE_URL1"]
SUPABASE_KEY1 = st.secrets["tutor"]["SUPABASE_KEY1"]
SUPABASE_URL2 = st.secrets["sessions"]["SUPABASE_URL2"]
SUPABASE_KEY2 = st.secrets["sessions"]["SUPABASE_KEY2"]

# Initialize Supabase client
supabase1: Client = create_client(SUPABASE_URL1, SUPABASE_KEY1)

# Client 2 (lowercase keys)
supabase2: Client = create_client(SUPABASE_URL2, SUPABASE_KEY2)


st.markdown("""
<style>
.ingestion-header {
    background: linear-gradient(to right, #6a0572, #ab83a1);
    padding: 25px 20px;
    border-radius: 12px;
    border: 2px solid red;
    box-shadow: 0 3px 6px rgba(0,0,0,0.1);
    color: white;
    margin-bottom: 30px;
}

.ingestion-header-content {
    display: flex;
    justify-content: center;
    align-items: center;
    flex-wrap: wrap;
    position: relative;
}

.ingestion-header-text {
    text-align: center;
    flex: 1;
}

.ingestion-header-text h1 {
    margin: 0;
    font-size: 30px;
    font-weight: 700;
    letter-spacing: 1px;
}

.ingestion-header-text p {
    margin: 6px 0 0;
    font-size: 14px;
    color: #fcefee;
    font-style: italic;
}

.ingestion-header img {
    position: absolute;
    right: 0;
    top: 50%;
    transform: translateY(-50%);
    max-height: 70px;
}
</style>

<div class="ingestion-header">
    <div class="ingestion-header-content">
        <div class="ingestion-header-text">
            <h1>A_STEP Data Ingestion Tracker</h1>
            <p>Monitoring data flow, completeness, and consistency — in real-time</p>
        </div>
        <img src="https://i.postimg.cc/zX2TGD2X/linegraph.png" alt="Tracker Logo">
    </div>
</div>
""", unsafe_allow_html=True)


# --- Get full date range from Supabase (only once)
date_range = supabase2.table("sessions").select('"Tutorial Date"').execute()

all_dates = [row["Tutorial Date"] for row in date_range.data if row["Tutorial Date"]]
min_date = min(all_dates)
max_date = max(all_dates)
st.sidebar.markdown('---')
st.sidebar.warning(f" 🗓️ Database Date Range")
st.sidebar.write(f"📅 Start Date: **{min_date}**")
st.sidebar.write(f"📅 End Date: **{max_date}**")
st.sidebar.markdown('---')

# Date inputs for user
start_date = st.sidebar.date_input("Select start date", value=pd.to_datetime(min_date))
end_date = st.sidebar.date_input("Select end date", value=pd.to_datetime(max_date))

# Convert dates to string format Supabase accepts
start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")

# User selects Faculty and Campus
faculty_choice = st.sidebar.selectbox("Select Faculty", ["All", "MEMS", "MHSC", "MTHL", "MNAS", "MHUM", "MLAW", "MEDU"])
campus_choice = st.sidebar.selectbox("Select Campus", ["All", "MAIN", "QWA", "SOUTH"])
    
st.sidebar.markdown("<h6 style='text-align: center; color: #196f3d;'> OneDrive Data (df1) </h6>", unsafe_allow_html=True)
df1_file = st.sidebar.file_uploader("", type=["csv"], label_visibility="collapsed")

# --- Validate user selection
if start_date_str < str(min_date) or end_date_str > str(max_date):
    st.warning(f"⚠️ Selected range is outside the available data ({min_date} → {max_date})")
elif start_date_str > end_date_str:
    st.error("❌ Start date cannot be after end date")
else:
    # --- Safe to query
    query = supabase2.table("sessions").select("*") \
        .gte("Tutorial Date", start_date_str) \
        .lte("Tutorial Date", end_date_str)
    data = query.execute()


    if faculty_choice != "All":
        query = query.eq("Acad Group", faculty_choice)

    if campus_choice != "All":
        query = query.eq("Campus", campus_choice)


    # fetch from "tutors" table
    response1 = supabase1.table("tutors").select("*").execute()
    db = pd.DataFrame(response1.data)

    # Query Supabase with date range
    # Apply ordering and range
    response2 = query.order("Tutorial Date").range(0, 4999).execute()

    attendance_df = pd.DataFrame(response2.data)



if df1_file and end_date_str <= str(max_date) and start_date_str <= end_date_str:
    st.sidebar.success('File Uploaded Successfully!')
    df1 = pd.read_csv(df1_file)
    df2 = attendance_df
    
    # Make unfiltered copies for metrics
    df1_original = df1.copy()
    df2_original = df2.copy()
    
    # Combine unique faculty names from both files
    all_faculties = pd.concat([df1['FACULTY'], df2['Acad Group']], ignore_index=True).dropna().unique()
    faculty_options = ["All Faculties"] + sorted(all_faculties)

    # Dropdown filter
    #selected_faculty = st.sidebar.selectbox("🎓 Filter by Faculty (optional):", faculty_options)

    # Apply filter only if a specific faculty is selected
    #if selected_faculty != "All Faculties":
        #df1 = df1[df1['FACULTY'] == selected_faculty]
        #df2 = df2[df2['Acad Group'] == selected_faculty]


    def convert_date_format(df1: pd.DataFrame) -> pd.DataFrame:
        """
        Converts the 'DATE' column in df1 from 'dd mm YYYY' format to 'mm/dd/yyyy' format.

        Args:
            df1 (pd.DataFrame): The DataFrame with 'DATE' column in 'dd mm YYYY' format.

        Returns:
            pd.DataFrame: The modified df1 with 'DATE' column in 'mm/dd/yyyy' format.
            """
            # Ensure 'DATE' column is treated as string before parsing, to avoid issues
            # if pandas tries to infer a different format initially.
        df1['DATE'] = df1['DATE'].astype(str)

        # Convert 'DATE' column in df1 to datetime objects.
        # The format code '%d %m %Y' tells pandas to parse 'day month year'.
        try:
            df1['DATE'] = pd.to_datetime(df1['DATE'], format='%d %m %Y')
        except ValueError as e:
            print(f"Error converting df1['DATE'] to datetime: {e}")
            print("Please check if the 'DATE' column in df1 strictly follows 'dd mm YYYY' format.")
            # If conversion fails, return the original df1 to prevent further errors.
            return df1

        # Convert the datetime objects back to string, but in the desired 'mm/dd/yyyy' format.
        # The format code '%m/%d/%Y' represents 'month/day/year'.
        df1['DATE'] = df1['DATE'].dt.strftime('%m/%d/%Y')

        return df1

    df3 = convert_date_format(df1.copy())  # ✅ Do this
    # For df2 (originally 'mm/dd/yyyy')
    df3['DATE'] = pd.to_datetime(df3['DATE'], format='%m/%d/%Y', errors='coerce')

    print(df3['DATE'].unique())
    #df1['DATE'] = pd.to_datetime(df1['DATE'], errors='coerce')
    df2['Tutorial Date'] = pd.to_datetime(df2['Tutorial Date'], errors='coerce')
    print(df2['Tutorial Date'].unique())
    
    # Parse df1 dates and show warning if any fail
    #df1['DATE'] = pd.to_datetime(df1['DATE'], errors='coerce')
    num_failed_df1 = df3['DATE'].isna().sum()
    if num_failed_df1 > 0:
        st.warning(f"⚠️ {num_failed_df1} dates in df1['DATE'] could not be parsed and were set to NaT.")

    # Parse df2 dates and show warning if any fail
    df2['Tutorial Date'] = pd.to_datetime(df2['Tutorial Date'], errors='coerce')
    num_failed_df2 = df2['Tutorial Date'].isna().sum()
    if num_failed_df2 > 0:
        st.warning(f"⚠️ {num_failed_df2} dates in df2['Tutorial Date'] could not be parsed and were set to NaT.")

    # Drop rows with NaT values if those dates are truly invalid/unwanted
    df3.dropna(subset=['DATE'], inplace=True)
    df2.dropna(subset=['Tutorial Date'], inplace=True)


    # Count attendance per day (not grouping by week)
    daily_df1 = df3.groupby('DATE')['STUDENT EMPLID'].count().reset_index(name='OneDrive Count')
    daily_df1['DATE'] = pd.to_datetime(daily_df1['DATE'])    
    daily_df2 = df2.groupby('Tutorial Date')['Attendee'].count().reset_index(name='PeopleSoft Count')
    daily_df2['Tutorial Date'] = pd.to_datetime(daily_df2['Tutorial Date'])
    # Merge daily counts
    merged = pd.merge(daily_df1, daily_df2, left_on='DATE', right_on='Tutorial Date', how='outer')
    merged = merged.dropna(subset=['DATE'])
    merged = merged.rename(columns={'DATE': 'Date'}).drop(columns=['Tutorial Date'])
    #merged = merged.sort_values('Date').fillna(0)
    merged['OneDrive Count'] = merged['OneDrive Count'].astype(int)
    #merged['PeopleSoft Count'] = merged['PeopleSoft Count'].astype(int)
    merged['PeopleSoft Count'] = pd.to_numeric(merged['PeopleSoft Count'], errors='coerce').fillna(0).astype(int)
    merged['Match %'] = (merged['PeopleSoft Count'] / merged['OneDrive Count']) * 100
    #merged['Match %'] = merged['Match %'].fillna(0).clip(upper=100)
    merged['OneDrive Count'] = merged['OneDrive Count'].fillna(0)
    merged['PeopleSoft Count'] = merged['PeopleSoft Count'].fillna(0)
    # Now safely convert to int
    merged['OneDrive Count'] = merged['OneDrive Count'].astype(int)
    #merged['PeopleSoft Count'] = merged['PeopleSoft Count'].astype(int)
    merged['PeopleSoft Count'] = pd.to_numeric(merged['PeopleSoft Count'], errors='coerce').fillna(0).astype(int)

    # OVERVIEW
    st.markdown("<h3 style='text-align: center; color: #2471a3;'>📊 Overview</h3>", unsafe_allow_html=True)
 

    total_uploaded = len(df3)
    total_ingested = len(df2)
    match_percent = (total_ingested / total_uploaded) * 100 if total_uploaded > 0 else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("OneDrive Attendance (df1)", total_uploaded)
    col2.metric("PeopleSof Attendance (df2)", total_ingested)    
    col3.metric(
        label="Total Match Rate",
        value=f"{match_percent:.2f}%",
        delta= None  # Optional
    )
    
    
    #col3.metric("Total Match Rate", f"{match_percent:.2f}%", delta=None)

    st.progress(min(match_percent / 100, 1.0))
    st.markdown("<h4 style='text-align: center; color: #2471a3;'>📈 Daily Attendance Trends</h4>", unsafe_allow_html=True)

    # LINE PLOT (DAILY)
    #st.subheader("📈 Daily Attendance Comparison")
    fig = px.line(
        merged,
        x='Date',
        y=['OneDrive Count', 'PeopleSoft Count'],
        markers=True,
        labels={'value': 'Attendance', 'Date': 'Date'},
        title=" "
    )
    # Center the title
    fig.update_layout(title_x=0.25)
    st.plotly_chart(fig, use_container_width=True)
     
    
    
    # 🔍 CAMPUS-WISE METRICS
    st.markdown("<h3 style='text-align: center; color: #2471a3;'>🏫 Campus-wise Upload Summary</h3>", unsafe_allow_html=True)


    # Normalize CAMPUS column casing
    df3['CAMPUS'] = df3['CAMPUS'].astype(str).str.strip().str.upper()
    #df2['Campus'] = df2['Campus'].astype(str).str.strip().str.upper()

    campuses = ['MAIN', 'QWA', 'SOUTH']
    campus_cols = st.columns(len(campuses))

    for i, campus in enumerate(campuses):
        uploaded = len(df3[df3['CAMPUS'] == campus])
        ingested = len(df2[df2['Campus'] == campus])
        percent = (ingested / uploaded * 100) if uploaded > 0 else 0
        delta_color = "normal" if percent >= 70 else ("inverse" if percent < 70 else "off")

        with campus_cols[i]:
            st.metric(
                label=f"{campus} Campus",
                value=f"{ingested} / {uploaded}",
                delta=f"{percent:.1f}%",
                delta_color=delta_color
            )
            st.progress(min(percent / 100, 1.0))

    st.markdown('---')

    # DAILY MATCH TABLE
    st.markdown("<h3 style='text-align: center; color: #2471a3;'>📅 Daily Match Rates</h3>", unsafe_allow_html=True)

    st.dataframe(merged.style.format({"Match %": "{:.2f}%"})
                         .background_gradient(subset=["Match %"], cmap="RdYlGn"), use_container_width=True)

   
    # ZERO DB ATTENDANCE
    # Header
    st.markdown("<h3 style='text-align: center; color: #2471a3;'>❌ Dates with No Database Records</h3>", unsafe_allow_html=True)

    # --- Define the merge keys ---
    # These are the columns used to determine if a row in df1 has a match in df2
    merge_keys = {
    'left_on': ['TUTOR EMPLID', 'DATE', 'CAMPUS', 'TERM'],
    'right_on': ['ID', 'Tutorial Date', 'Campus', 'Term']
    }
    ## convert datatypes before merging..
    
    df2["ID"] = df2["ID"].astype(str)
    df3["TUTOR EMPLID"] = df3["TUTOR EMPLID"].astype(str)

    df2["Term"] = df2["Term"].astype(str)
    df3["TERM"] = df3["TERM"].astype(str)  
    
    # --- Perform a left merge ---
    # A left merge keeps all rows from df1 and adds matching columns from df2.
    # If no match is found in df2, the columns from df2 will be NaN.
    merged_df = df3.merge(
        df2,
        left_on=merge_keys['left_on'],
        right_on=merge_keys['right_on'],
        how='left',
        indicator=True # This adds a '_merge' column indicating the source of the row
    )

    # --- Filter for rows that are only in df1 (anti-join) ---
    # The '_merge' column will have 'left_only' for rows that exist only in the left DataFrame (df1)
    df1_not_in_df2 = merged_df[merged_df['_merge'] == 'left_only'].copy()

    # Drop the '_merge' column and the merged columns from df2 (if you don't need them)
    # We only want the original df1 columns for the result.
    df1_not_in_df2 = df1_not_in_df2[df1.columns]

    # --- Display the result ---
    #print("DataFrame 1 (df1):\n", df1)
    #print("\nDataFrame 2 (df2):\n", df2)
    #print("\nEntries in df1 that are NOT in df2 (df1_not_in_df2):\n", df1_not_in_df2)
    if not df1_not_in_df2.empty:
        st.dataframe(df1_not_in_df2, use_container_width=True)
        df1_not_in_df2['Module Code'] = df1_not_in_df2['MODULE'].astype(str).str.strip() + df1_not_in_df2['CODE'].astype(str).str.strip()

        @st.cache_data
        def fuzzy_match_modules(missing_df, reference_df, threshold=80):
            missing_modules = missing_df['Module Code'].dropna().unique()
            reference_modules = reference_df['module_id'].dropna().unique()

            results = []

            for module in missing_modules:
                best_match, score, _ = process.extractOne(
                    module,
                    reference_modules,
                    scorer=fuzz.token_sort_ratio
                )

                if score >= threshold:
                    results.append({
                        'Missing Module': module,
                        'Possible Match in DB': best_match,
                        'Match Score': score
                    })
                else:
                        results.append({
                        'Missing Module': module,
                        'Possible Match in DB': None,
                        'Match Score': score
                    })

            return pd.DataFrame(results)

        match_results = fuzzy_match_modules(df1_not_in_df2, db)
        st.markdown("<h3 style='text-align: center; color: #2471a3;'>🔍 Fuzzy Matched Modules</h3>", unsafe_allow_html=True)
        st.dataframe(
            match_results.sort_values("Match Score", ascending=False)
            .style.format({"Match Score": "{:.2f}%"})
            .background_gradient(subset=["Match Score"], cmap="RdYlGn"),
            use_container_width=True
        )

        @st.cache_data
        def fuzzy_match_tutor_ids(df_missing, db):
            results = []

            db_ids = db['tutor_id'].astype(str).dropna().unique()

            for tutor_id in df_missing['TUTOR EMPLID'].dropna().astype(str).unique():
                match, score, _ = process.extractOne(
                    query=tutor_id,
                    choices=db_ids,
                    scorer=fuzz.ratio
                )
                results.append({
                    "TUTOR EMPLID (Missing)": tutor_id,
                    "Closest Match in DB": match,
                    "Match Score": score
                })

            return pd.DataFrame(results)

        tutor_match_results = fuzzy_match_tutor_ids(df1_not_in_df2, db)

        #st.subheader("🧑‍🏫 Fuzzy Match: TUTOR EMPLID vs. SQL Database")
        st.markdown("<h3 style='text-align: center; color: #2471a3;'>🧑‍🏫 Fuzzy Matched Tutors</h3>", unsafe_allow_html=True)

        st.dataframe(
            tutor_match_results.sort_values("Match Score", ascending=False)
            .style.format({"Match Score": "{:.2f}%"})
            .background_gradient(subset=["Match Score"], cmap="RdYlGn"),
            use_container_width=True
        )


    else:
        st.success("✅ All records in df1 exist in df2.")

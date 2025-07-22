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

st.set_page_config(layout="wide")
#st.markdown("![Alt Text](https://i.postimg.cc/yNn12Vj9/header.png)")

st.markdown("""
<style>
.ingestion-header {
    background: linear-gradient(to right, #6a0572, #ab83a1);
    padding: 25px 20px;
    border-radius: 12px;
    border: 2px solid red;
    box-shadow: 0 3px 6px rgba(0,0,0,0.1);
    text-align: center;
    color: white;
    margin-bottom: 30px;
}
.ingestion-header h1 {
    margin: 0;
    font-size: 30px;
    font-weight: 700;
    letter-spacing: 1px;
}
.ingestion-header p {
    margin: 6px 0 0;
    font-size: 14px;
    color: #fcefee;
    font-style: italic;
}
</style>

<div class="ingestion-header">
    <h1>A_STEP Data Ingestion Tracker</h1>
    <p>Monitoring data flow, completeness, and consistency — in real-time</p>
</div>
""", unsafe_allow_html=True)


# Sidebar file upload
st.sidebar.markdown("""
<style>
.sidebar-header {
    background-color: white;
    padding: 15px;
    margin-bottom: 10px;
    border-left: 8px solid purple;
    border-radius: 5px;
    box-shadow: 0 5px 9px rgba(0,0,0,0.1);
}
.sidebar-header h2 {
    font-size: 16px;
    color: purple;
    margin: 0;
    display: flex;
    align-items: center;
}
.sidebar-header h3 span {
    margin-right: 8px;
}
.sidebar-header p {
    font-size: 13px;
    color: #555;
    margin-top: 5px;
    margin-bottom: 0;
}
</style>

<div class="sidebar-header">
    <h2><span>📁</span> Upload Data Files</h2>
</div>
""", unsafe_allow_html=True)

st.sidebar.markdown("""
<style>
.upload-box {
    padding: 15px;
    margin-top: 10px;
    margin-bottom: 20px;
    background-color: #800080;
    border: 1px dashed #007acc;
    border-radius: 6px;
}
.upload-box h4 {
    margin: 0;
    font-size: 15px;
    color: #800080;
}
.upload-box p {
    font-size: 13px;
    color: #800080;
    margin-top: 5px;
}
</style>

<div class="upload-box">
    <h4>📤 OneDrive Data (df1) 📤</h4>
</div>
""", unsafe_allow_html=True)

df1_file = st.sidebar.file_uploader("", type=["csv"], label_visibility="collapsed")

st.sidebar.markdown("""
<style>
.upload-box {
    padding: 15px;
    margin-top: 10px;
    margin-bottom: 20px;
    background-color: #f0f4f8;
    border: 1px dashed #007acc;
    border-radius: 6px;
}
.upload-box h4 {
    margin: 0;
    font-size: 15px;
    color: #004080;
}
.upload-box p {
    font-size: 13px;
    color: #333;
    margin-top: 5px;
}
</style>

<div class="upload-box">
    <h4>📤 PeopleSoft Data (df2)</h4>
</div>
""", unsafe_allow_html=True)

df2_file = st.sidebar.file_uploader("PeopleSoft Data (df2)", type=["csv"])

if df1_file and df2_file:
    df1 = pd.read_csv(df1_file)
    df2 = pd.read_csv(df2_file)
    
    # Make unfiltered copies for metrics
    df1_original = df1.copy()
    df2_original = df2.copy()
    
    # Combine unique faculty names from both files
    all_faculties = pd.concat([df1['FACULTY'], df2['Acad Group']], ignore_index=True).dropna().unique()
    faculty_options = ["All Faculties"] + sorted(all_faculties)

    # Dropdown filter
    selected_faculty = st.sidebar.selectbox("🎓 Filter by Faculty (optional):", faculty_options)

    # Apply filter only if a specific faculty is selected
    if selected_faculty != "All Faculties":
        df1 = df1[df1['FACULTY'] == selected_faculty]
        df2 = df2[df2['Acad Group'] == selected_faculty]


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
    st.markdown("<h3 style='text-align: center;'>📊 Overview</h3>", unsafe_allow_html=True)

    total_uploaded = len(df3)
    total_ingested = len(df2)
    match_percent = (total_ingested / total_uploaded) * 100 if total_uploaded > 0 else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("OneDrive Attendance (df1)", total_uploaded)
    col2.metric("PeopleSof Attendance (df2)", total_ingested)    
    col3.metric("Total Match Rate", f"{match_percent:.2f}%", delta=None)

    st.progress(min(match_percent / 100, 1.0))
    #st.markdown('---')

    # LINE PLOT (DAILY)
    #st.subheader("📈 Daily Attendance Comparison")
    fig = px.line(
        merged,
        x='Date',
        y=['OneDrive Count', 'PeopleSoft Count'],
        markers=True,
        labels={'value': 'Attendance', 'Date': 'Date'},
        title="Daily Attendance: OneDrive vs. PeopleSoft"
    )
    # Center the title
    fig.update_layout(title_x=0.25)
    st.plotly_chart(fig, use_container_width=True)
    # 🔍 CAMPUS-WISE METRICS
    st.markdown("<h3 style='text-align: center;'>🏫 Campus-wise Upload Summary</h3>", unsafe_allow_html=True)


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
    st.markdown("<h3 style='text-align: center;'>📅 Daily Match Rates</h3>", unsafe_allow_html=True)

    st.dataframe(merged.style.format({"Match %": "{:.2f}%"})
                         .background_gradient(subset=["Match %"], cmap="RdYlGn"), use_container_width=True)

   
    # ZERO DB ATTENDANCE
    #st.subheader("❌ Dates with No Database Records")
    # Header
    st.markdown("<h3 style='text-align: center;'>❌ Dates with No Database Records</h3>", unsafe_allow_html=True)

    # --- Define the merge keys ---
    # These are the columns used to determine if a row in df1 has a match in df2
    merge_keys = {
    'left_on': ['TUTOR EMPLID', 'DATE', 'CAMPUS', 'TERM'],
    'right_on': ['ID', 'Tutorial Date', 'Campus', 'Term']
    }

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
    print("DataFrame 1 (df1):\n", df1)
    print("\nDataFrame 2 (df2):\n", df2)
    print("\nEntries in df1 that are NOT in df2 (df1_not_in_df2):\n", df1_not_in_df2)
    if not df1_not_in_df2.empty:
        st.dataframe(df1_not_in_df2, use_container_width=True)
    else:
        st.success("✅ All records in df1 exist in df2.")

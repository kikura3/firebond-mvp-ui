import time  # to simulate a real time data, time loop

import numpy as np  # np mean, np random
import pandas as pd  # read csv, df manipulation
import plotly.express as px  # interactive charts
import streamlit as st  # 🎈 data web app development
import pymysql
from sqlalchemy import create_engine
import plotly.express as px  # interactive charts


st.set_page_config(
    page_title="FireBond Analytics",
    page_icon="📊",
    layout="wide",
)

st.title("FireBond Analytics")

hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            footer:after {
                content:'Developed with ❤ by firebond.xyz'; 
                visibility: visible;
                display: block;
                position: relative;
                padding: 5px;
                top: 2px;
            }
            </style>
            """

st.markdown(hide_streamlit_style, unsafe_allow_html=True)

DB_CONN_URL = st.secrets["DB_CONN_URL"]

@st.experimental_memo(ttl=120)
def get_user_info() -> pd.DataFrame:

    sql = """
    WITH user_info AS 
    (
    SELECT
        SUM(
        CASE
            WHEN
                joined_at >= date_sub(now(), INTERVAL 7 DAY) 
            THEN
                1 
            ELSE
                0 
        END
    ) AS new_users, COUNT(*) AS total_users 
    FROM
        members 
    )
    SELECT
    total_users,
    new_users / (total_users - new_users + 1) AS change_pct 
    FROM
    user_info
    """
    return run_query(sql)


@st.experimental_memo(ttl=120)
def get_active_user_info() -> pd.DataFrame:

    sql = """
    WITH author_info_1 AS 
    (
    SELECT
        COUNT(DISTINCT author_id) AS prev_week_active_users 
    FROM
        firebond.messages 
    WHERE
        created_at >= DATE(date_sub(now(), INTERVAL 14 DAY)) 
        AND created_at < DATE(date_sub(now(), INTERVAL 7 DAY)) 
    )
    ,
    author_info_2 AS 
    (
    SELECT
        COUNT(DISTINCT author_id) AS curr_week_active_users 
    FROM
        firebond.messages 
    WHERE
        created_at >= DATE(date_sub(now(), INTERVAL 7 DAY)) 
    )
    SELECT
    curr_week_active_users AS active_users,
    curr_week_active_users / (prev_week_active_users + 1) AS change_pct 
    FROM
    author_info_1 
    INNER JOIN
        author_info_2 
        ON 1 = 1
    """
    return run_query(sql) 

@st.experimental_memo(ttl=120)
def get_interactions_info() -> pd.DataFrame:

    sql = """
    WITH msg_info_1 AS 
    (
    SELECT
        COUNT(*) AS prev_week_messages 
    FROM
        firebond.messages 
    WHERE
        created_at >= DATE(date_sub(now(), INTERVAL 14 DAY)) 
        AND created_at < DATE(date_sub(now(), INTERVAL 7 DAY)) 
    )
    ,
    msg_info_2 AS 
    (
    SELECT
        COUNT(*) AS curr_week_messages 
    FROM
        firebond.messages 
    WHERE
        created_at >= DATE(date_sub(now(), INTERVAL 7 DAY)) 
    )
    SELECT
    curr_week_messages AS interactions,
    curr_week_messages / (prev_week_messages + 1) AS change_pct 
    FROM
    msg_info_1 
    INNER JOIN
        msg_info_2 
        ON 1 = 1
    """
    return run_query(sql) 

@st.experimental_memo(ttl=120)
def get_members_engagement() -> pd.DataFrame:

    sql = """
    WITH member_info_1 AS 
    (
    SELECT
        member_id,
        COUNT(DISTINCT channel_id) AS num_channels 
    FROM
        channel_members 
    GROUP BY
        member_id 
    )
    ,
    member_info_2 AS 
    (
    SELECT
        author_id,
        COUNT(*) AS num_messages 
    FROM
        messages 
    WHERE
        created_at >= DATE(date_sub(now(), INTERVAL 7 DAY)) 
    GROUP BY
        author_id 
    )
    ,
    member_info_3 AS 
    (
    SELECT
        author_id,
        COUNT(*) AS num_messages_last_week 
    FROM
        messages 
    WHERE
        created_at >= DATE(date_sub(now(), INTERVAL 14 DAY)) 
        AND created_at < DATE(date_sub(now(), INTERVAL 7 DAY)) 
    GROUP BY
        author_id 
    )
    SELECT
        m.name,
        m1.num_channels,
        COALESCE(m2.num_messages,0),
        COALESCE(m2.num_messages / (
        SELECT
            SUM(num_messages) 
        FROM
            member_info_2),0) AS engagement_rate,
        COALESCE(m2.num_messages / COALESCE(m3.num_messages_last_week, 1),0) AS change_pct,
        DATE(m.joined_at) AS joined_at
    FROM
        member_info_1 m1 
        LEFT JOIN
            member_info_2 m2 
            ON m1.member_id = m2.author_id 
        LEFT JOIN
            member_info_3 m3 
            ON m1.member_id = m3.author_id 
        INNER JOIN
            members m 
            ON m1.member_id = m.id
    """
    return run_query(sql) 


@st.experimental_memo(ttl=120)
def get_active_users_daily() -> pd.DataFrame:

    sql = """
    WITH all_dates AS 
    (
    SELECT
        a.DATE 
    FROM
        (
            SELECT
                curdate() - INTERVAL (a.a + (10 * b.a) + (100 * c.a) + (1000 * d.a) ) DAY AS DATE 
            FROM
                (
                SELECT
                    0 AS a 
                UNION ALL
                SELECT
                    1 
                UNION ALL
                SELECT
                    2 
                UNION ALL
                SELECT
                    3 
                UNION ALL
                SELECT
                    4 
                UNION ALL
                SELECT
                    5 
                UNION ALL
                SELECT
                    6 
                UNION ALL
                SELECT
                    7 
                UNION ALL
                SELECT
                    8 
                UNION ALL
                SELECT
                    9
                )
                AS a 
                CROSS JOIN
                (
                    SELECT
                        0 AS a 
                    UNION ALL
                    SELECT
                        1 
                    UNION ALL
                    SELECT
                        2 
                    UNION ALL
                    SELECT
                        3 
                    UNION ALL
                    SELECT
                        4 
                    UNION ALL
                    SELECT
                        5 
                    UNION ALL
                    SELECT
                        6 
                    UNION ALL
                    SELECT
                        7 
                    UNION ALL
                    SELECT
                        8 
                    UNION ALL
                    SELECT
                        9
                )
                AS b 
                CROSS JOIN
                (
                    SELECT
                        0 AS a 
                    UNION ALL
                    SELECT
                        1 
                    UNION ALL
                    SELECT
                        2 
                    UNION ALL
                    SELECT
                        3 
                    UNION ALL
                    SELECT
                        4 
                    UNION ALL
                    SELECT
                        5 
                    UNION ALL
                    SELECT
                        6 
                    UNION ALL
                    SELECT
                        7 
                    UNION ALL
                    SELECT
                        8 
                    UNION ALL
                    SELECT
                        9
                )
                AS c 
                CROSS JOIN
                (
                    SELECT
                        0 AS a 
                    UNION ALL
                    SELECT
                        1 
                    UNION ALL
                    SELECT
                        2 
                    UNION ALL
                    SELECT
                        3 
                    UNION ALL
                    SELECT
                        4 
                    UNION ALL
                    SELECT
                        5 
                    UNION ALL
                    SELECT
                        6 
                    UNION ALL
                    SELECT
                        7 
                    UNION ALL
                    SELECT
                        8 
                    UNION ALL
                    SELECT
                        9
                )
                AS d 
        )
        a 
    WHERE
        a.DATE BETWEEN DATE(date_sub(now(), INTERVAL 7 DAY)) AND DATE(now()) 
    )
    ,
    active_user_info AS 
    (
    SELECT DISTINCT
        author_id,
        DATE(created_at) AS created_at 
    FROM
        messages 
    WHERE
        created_at >= DATE(date_sub(now(), INTERVAL 14 DAY)) 
    )
    ,
    active_user_info_2 AS 
    (
    SELECT
        created_at,
        COUNT(*) AS num_active_users 
    FROM
        active_user_info 
    GROUP BY
        created_at 
    )
    SELECT
    ad.DATE AS dt,
    COALESCE(num_active_users, 0) AS num_active_users 
    FROM
    all_dates ad 
    LEFT JOIN
        active_user_info_2 au 
        ON ad.DATE = au.created_at
    """
    return run_query(sql) 

def run_query(sql):

    engine = create_engine(DB_CONN_URL, pool_recycle=3600)
    df = pd.read_sql(sql, engine)
    return df

rad = st.sidebar.radio("Navigation",["Dashboard","Trends"])

if rad == "Dashboard":

    col1, col2, col3 = st.columns(3)

    df_u = get_user_info()

    total_users = str(int(df_u.total_users))
    user_change = str(round(df_u.change_pct*100, 1).iloc[0]) + " % since last week"
    col1.metric("Discord Users", total_users, user_change, help="")

    df_au = get_active_user_info()

    active_users = str(int(df_au.active_users))
    active_user_change = str(round(df_au.change_pct*100, 1).iloc[0]) + " % since last week"
    col2.metric("Active Users", active_users, active_user_change, help="active users this week")

    df_i = get_interactions_info()

    interactions = str(int(df_i.interactions))
    interactions_change = str(round(df_i.change_pct*100, 1).iloc[0]) + " % since last week"
    col3.metric("Interactions", interactions, interactions_change, help="interactions this week")


    df_au_daily = get_active_users_daily()
    
    st.subheader("Daily Active Users")
    trending_fig = px.line(df_au_daily, x='dt', y='num_active_users', markers=True)
    trending_fig.update_layout(xaxis=dict(showgrid=False), yaxis=dict(showgrid=False))
    trending_fig.update_layout(xaxis_title="Day", yaxis_title="Active Users")
    st.plotly_chart(trending_fig, use_container_width=True)

    df_m = get_members_engagement()
    df_m.columns = ['Member', 'Channels', 'Messages', 'Engagement Rate', 'Change Since Last Week', 'Date Joined']
    
    st.subheader("Members Engagement (last 7 days)")
    st.dataframe(df_m.set_index('Member').sort_values('Engagement Rate', ascending=False)
                        .style
                        .format({'Engagement Rate': "{:.1%}", 'Change Since Last Week': "{:.1%}"})
                        .background_gradient(cmap='YlGn', axis=0, subset=['Engagement Rate','Change Since Last Week']),
                 use_container_width=True
                )

if rad == "Trends":

    st.text('Coming soon!')

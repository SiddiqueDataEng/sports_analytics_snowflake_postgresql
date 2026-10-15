# dashboard/sf_connection.py
import streamlit as st
import snowflake.connector

def get_snowflake_connection():
    cfg = st.secrets["snowflake"]

    conn = st.session_state.get("sf_conn")
    if conn is not None and not conn.is_closed():
        return conn

    with st.sidebar:
        st.subheader("Snowflake login")
        code = st.text_input("Authenticator code (6 digits)", max_chars=6)
        connect_clicked = st.button("Connect")

    if not connect_clicked:
        st.info("Enter your current authenticator code in the sidebar and click Connect.")
        st.stop()

    try:
        conn = snowflake.connector.connect(
            account=cfg["account"],
            user=cfg["user"],
            password=cfg["password"],
            passcode=code.strip(),
            role=cfg.get("role"),
            warehouse=cfg["warehouse"],
            database=cfg["database"],
            schema=cfg["schema"],
        )
    except Exception as e:
        st.error(f"Snowflake login failed: {e}")
        st.stop()

    st.session_state["sf_conn"] = conn
    return conn

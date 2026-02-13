"""
Flask web application for visualizing Sofia air quality data.
Queries PostgreSQL and renders Plotly charts.
"""

import os
import json
import pandas as pd
import plotly
import plotly.express as px
from flask import Flask, render_template, request, jsonify
from psycopg2 import pool

app = Flask(__name__)

DB_POOL = None


def get_db_pool():
    global DB_POOL
    if DB_POOL is None:
        DB_POOL = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            host=os.environ.get("DB_HOST", "postgres"),
            port=os.environ.get("DB_PORT", "5432"),
            dbname=os.environ.get("DB_NAME", "airquality"),
            user=os.environ.get("DB_USER", "airflow"),
            password=os.environ.get("DB_PASSWORD", "airflow"),
        )
    return DB_POOL


def query_db(sql, params=None):
    db = get_db_pool()
    conn = db.getconn()
    try:
        df = pd.read_sql_query(sql, conn, params=params)
        return df
    finally:
        db.putconn(conn)


POLLUTANT_NAMES = {"5": "PM2.5", "6001": "PM10"}


@app.route("/")
def index():
    # Get available stations
    stations_df = query_db(
        "SELECT DISTINCT station_code FROM air_quality_measurements ORDER BY station_code"
    )
    stations = stations_df["station_code"].tolist() if not stations_df.empty else []

    # Filter parameters
    selected_station = request.args.get("station", "all")
    selected_pollutant = request.args.get("pollutant", "all")

    where_clauses = []
    params = {}
    if selected_station != "all":
        where_clauses.append("station_code = %(station)s")
        params["station"] = selected_station
    if selected_pollutant != "all":
        where_clauses.append("pollutant = %(pollutant)s")
        params["pollutant"] = selected_pollutant

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    # Daily averages for the time series chart
    timeseries_sql = f"""
        SELECT
            DATE(datetime) as date,
            pollutant,
            AVG(value) as avg_value
        FROM air_quality_measurements
        {where_sql}
        GROUP BY DATE(datetime), pollutant
        ORDER BY date
    """
    ts_df = query_db(timeseries_sql, params)

    # Summary statistics
    stats_sql = f"""
        SELECT
            pollutant,
            COUNT(*) as total_measurements,
            ROUND(AVG(value)::numeric, 2) as avg_value,
            ROUND(MIN(value)::numeric, 2) as min_value,
            ROUND(MAX(value)::numeric, 2) as max_value,
            MIN(datetime)::date as earliest,
            MAX(datetime)::date as latest,
            COUNT(DISTINCT station_code) as station_count
        FROM air_quality_measurements
        {where_sql}
        GROUP BY pollutant
    """
    stats_df = query_db(stats_sql, params)

    # Build Plotly chart
    timeseries_chart = "{}"
    if not ts_df.empty:
        ts_df["pollutant_name"] = ts_df["pollutant"].map(POLLUTANT_NAMES).fillna(ts_df["pollutant"])
        fig = px.line(
            ts_df,
            x="date",
            y="avg_value",
            color="pollutant_name",
            title="Daily Average PM2.5 and PM10 Concentrations",
            labels={
                "date": "Date",
                "avg_value": "Concentration (ug/m3)",
                "pollutant_name": "Pollutant",
            },
        )
        fig.update_layout(template="plotly_white", hovermode="x unified", height=500)
        timeseries_chart = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)

    stats = stats_df.to_dict("records") if not stats_df.empty else []

    return render_template(
        "index.html",
        timeseries_chart=timeseries_chart,
        stats=stats,
        stations=stations,
        selected_station=selected_station,
        selected_pollutant=selected_pollutant,
        pollutant_names=POLLUTANT_NAMES,
    )


@app.route("/health")
def health():
    try:
        query_db("SELECT 1")
        return jsonify({"status": "healthy"}), 200
    except Exception as e:
        return jsonify({"status": "unhealthy", "error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

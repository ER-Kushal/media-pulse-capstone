"""API test cases (run with:  cd api && pytest -v).
Needs the warehouse loaded + dbt marts + ML tables + alerts (the CI workflow builds all of that)."""
import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def token(user, pw):
    r = client.post("/api/auth/login", data={"username": user, "password": pw})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


@pytest.fixture(scope="module")
def admin():
    return token("admin", "admin123")


@pytest.fixture(scope="module")
def viewer():
    return token("viewer", "viewer123")


# ---- health & auth --------------------------------------------------------
def test_health_is_public():
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["database"] == "up"


def test_login_wrong_password_rejected():
    assert client.post("/api/auth/login", data={"username": "admin", "password": "nope"}).status_code == 401


def test_protected_endpoint_requires_token():
    assert client.get("/api/dashboard").status_code == 401


def test_bad_token_rejected():
    assert client.get("/api/dashboard", headers={"Authorization": "Bearer garbage"}).status_code == 401


# ---- the 12 endpoints -----------------------------------------------------
def test_audience_live(admin):
    r = client.get("/api/audience/live?minutes=60", headers=admin)
    assert r.status_code == 200
    body = r.json()
    assert "concurrent_users_now" in body and "latest_event_at" in body and isinstance(body["by_device"], list)


def test_audience_live_validation(admin):
    assert client.get("/api/audience/live?minutes=0", headers=admin).status_code == 422
    assert client.get("/api/audience/live?minutes=99999", headers=admin).status_code == 422


def test_content_performance(admin):
    top = client.get("/api/content?limit=1", headers=admin).json()[0]["content_id"]
    r = client.get(f"/api/content/{top}/performance", headers=admin)
    assert r.status_code == 200
    body = r.json()
    assert body["content_id"] == top and body["rank_by_watch_time"] >= 1 and "daily_trend" in body


def test_content_not_found_and_bad_id(admin):
    assert client.get("/api/content/ZZZ999/performance", headers=admin).status_code == 404
    assert client.get("/api/content/bad%20id!/performance", headers=admin).status_code == 422


def test_subscriber_health(admin):
    uid = client.get("/api/churn-risk?limit=1", headers=admin).json()["items"][0]["user_id"]
    r = client.get(f"/api/subscriber/{uid}/health", headers=admin)
    assert r.status_code == 200
    body = r.json()
    assert body["profile"]["user_id"] == uid and body["health_label"] in ("healthy", "at_risk", "critical")
    assert "total_tickets" in body["support"]
    assert client.get("/api/subscriber/NOPE1/health", headers=admin).status_code == 404


def test_churn_risk_filter_and_pagination(admin):
    r = client.get("/api/churn-risk?tier=high&limit=5", headers=admin)
    assert r.status_code == 200
    body = r.json()
    assert len(body["items"]) <= 5 and all(i["risk_tier"] == "high" for i in body["items"])
    assert client.get("/api/churn-risk?tier=extreme", headers=admin).status_code == 422


def test_campaigns(admin):
    r = client.get("/api/campaigns?limit=5", headers=admin)
    assert r.status_code == 200 and len(r.json()) <= 5
    assert {"campaign_id", "ctr", "ad_fill_rate"} <= set(r.json()[0])


def test_dashboard(admin):
    r = client.get("/api/dashboard", headers=admin)
    assert r.status_code == 200
    k = r.json()["kpis"]
    assert k["total_views"] > 0 and k["subscribers_total"] > 0 and len(r.json()["watch_time_trend"]) > 0


def test_events_live(admin):
    r = client.get("/api/events/live?limit=10", headers=admin)
    assert r.status_code == 200 and 0 < r.json()["count"] <= 10


def test_alerts_list_and_filters(admin):
    r = client.get("/api/alerts?limit=5", headers=admin)
    assert r.status_code == 200 and r.json()["total"] >= 1
    r2 = client.get("/api/alerts?alert_type=content_alert&status=open", headers=admin)
    assert all(a["alert_type"] == "content_alert" for a in r2.json()["items"])
    assert client.get("/api/alerts?status=bogus", headers=admin).status_code == 422


def test_forecasts(admin):
    r = client.get("/api/forecasts", headers=admin)
    assert r.status_code == 200 and len(r.json()["forecast"]) == 7


def test_risk(admin):
    r = client.get("/api/risk", headers=admin)
    assert r.status_code == 200 and r.json()["overall_risk_level"] in ("low", "medium", "high")


def test_data_quality(admin):
    r = client.get("/api/data-quality", headers=admin)
    assert r.status_code == 200
    body = r.json()
    assert body["overall_status"] in ("pass", "warn", "fail") and len(body["checks"]) >= 8
    # hard integrity rules must always pass
    hard = {c["check"]: c["status"] for c in body["checks"]}
    assert hard["duplicate event_id in fact_view"] == "pass"
    assert hard["business rule: click implies impression"] == "pass"


# ---- authorization + audit trail -----------------------------------------
def test_acknowledge_requires_privileged_role(admin, viewer):
    open_alert = client.get("/api/alerts?status=open&limit=1", headers=admin).json()["items"][0]["alert_id"]
    assert client.post(f"/api/alerts/{open_alert}/acknowledge", headers=viewer).status_code == 403
    assert client.post(f"/api/alerts/{open_alert}/acknowledge").status_code == 401


def test_acknowledge_flow_and_audit_log(admin):
    open_alert = client.get("/api/alerts?status=open&limit=1", headers=admin).json()["items"][0]["alert_id"]
    r = client.post(f"/api/alerts/{open_alert}/acknowledge", headers=admin, json={"note": "pytest ack"})
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "acknowledged" and body["owner"] == "admin"
    assert body["audit_log"][-1]["event"] == "acknowledged" and body["audit_log"][-1]["note"] == "pytest ack"
    # second attempt -> conflict
    assert client.post(f"/api/alerts/{open_alert}/acknowledge", headers=admin).status_code == 409


def test_acknowledge_unknown_alert(admin):
    assert client.post("/api/alerts/99999999/acknowledge", headers=admin).status_code == 404

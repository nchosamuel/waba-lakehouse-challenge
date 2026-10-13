"""Local banking data generation UI."""

import time
from datetime import date, datetime, timedelta
import streamlit as st
from waba.generator import COUNTRIES
from waba.contracts import FLOW_PREFIX
from waba.publisher import publish
from waba.full_references import publish_full_references

st.set_page_config(page_title="WABA — Génération", page_icon="🏦", layout="wide")
st.title("WABA · Génération de données")
st.caption("Données fictives de banque et assurance · Afrique de l’Ouest")
st.info(
    "Quatre flux disponibles. Les référentiels complets du groupe peuvent être générés séparément : 500 000 clients et 800 000 comptes."
)
if "running" not in st.session_state:
    st.session_state.running = False
    st.session_state.history = []
    st.session_state.last_run = 0.0
    st.session_state.error = None
running = st.session_state.running
labels = {
    "Transactions bancaires": "bank_transactions",
    "Opérations d’assurance": "insurance_operations",
    "Paiements mobile money": "mobile_money_payments",
    "Remboursements de crédit": "loan_repayments",
    "Référentiels": "references",
}
choice = st.selectbox("Type de données", list(labels), disabled=running)
dataset = labels[choice]
entity_options = {
    "bank_transactions": ["BANK", "MICROFINANCE"],
    "insurance_operations": ["INSURANCE"],
    "mobile_money_payments": ["MOBILE_MONEY"],
    "loan_repayments": ["MICROFINANCE"],
    "references": ["ALL"],
}
entity_labels = {
    "BANK": "Banque",
    "MICROFINANCE": "Microfinance",
    "INSURANCE": "Assurance",
    "MOBILE_MONEY": "Mobile money",
    "ALL": "Les quatre lignes métier",
}
entity = st.selectbox(
    "Ligne métier",
    entity_options[dataset],
    format_func=entity_labels.get,
    disabled=running,
)
full_refs = (
    st.checkbox("Volumes complets du groupe · les 8 pays", disabled=running)
    if dataset == "references"
    else False
)
countries = st.multiselect(
    "Pays", COUNTRIES, default=["CI"], disabled=running or full_refs
)
rows = st.number_input(
    "Lignes par pays et par lot",
    min_value=1,
    max_value=100000,
    value={
        "bank_transactions": 10000,
        "insurance_operations": 5000,
        "mobile_money_payments": 20000,
        "loan_repayments": 10000,
        "references": 1,
    }[dataset],
    step=100,
    key=f"rows-{dataset}",
    disabled=running,
)
today = date.today()
quarter_start = date(today.year, ((today.month - 1) // 3) * 3 + 1, 1)
previous_end = quarter_start - timedelta(days=1)
previous_start = date(previous_end.year, ((previous_end.month - 1) // 3) * 3 + 1, 1)
left, right = st.columns(2)
start = left.date_input("Début de période", previous_start, disabled=running)
end = right.date_input("Fin de période", previous_end, disabled=running)
if dataset == "references":
    st.caption(
        "Le nombre de lignes saisi s’applique aux flux. Les référentiels complets sont conservés lorsqu’ils existent déjà. Le profil complet génère 500 000 clients, 800 000 comptes, 200 agences et 50 produits au total sur les 8 pays."
    )
mode = st.radio(
    "Mode de génération",
    ["Une fois"] if full_refs else ["Une fois", "Continue"],
    horizontal=True,
    disabled=running,
)
interval = st.slider(
    "Intervalle entre les lots (secondes)", 10, 60, 30, disabled=running
)
st.caption(
    "Les fichiers sont déposés dans MinIO. Airflow peut traiter les fichiers toutes les 15 minutes lorsqu’il est activé. Le mode continu fonctionne pendant que cette session reste ouverte."
)
valid = full_refs or (bool(countries) and end >= start)
if not valid:
    st.warning("Choisis au moins un pays et une période valide.")


def produce():
    try:
        if full_refs:
            full_report = publish_full_references()
            report = {
                "batch_id": "full-v1",
                "rows": sum(full_report["totals"].values()),
                "files": full_report["files"],
            }
        else:
            report = publish(
                countries,
                int(rows),
                datetime.combine(start, datetime.min.time()),
                datetime.combine(end, datetime.min.time()),
                dataset=dataset,
                entity_type=entity,
            )
        st.session_state.history.insert(
            0,
            {
                "Lot": report["batch_id"],
                "Type": choice,
                "Lignes": report["rows"],
                "Fichiers": len(report["files"]),
                "Pays": ", ".join(COUNTRIES if full_refs else countries),
                "Ligne métier": entity_labels[entity],
            },
        )
        st.session_state.history = st.session_state.history[:20]
        st.session_state.error = None
    except Exception as exc:
        st.session_state.error = str(exc)
        st.session_state.running = False
    st.session_state.last_run = time.monotonic()


if mode == "Une fois":
    if st.button("Générer et envoyer vers MinIO", type="primary", disabled=not valid):
        with st.spinner("Génération et dépôt des fichiers…"):
            produce()
else:
    a, b = st.columns(2)
    if a.button("Démarrer", type="primary", disabled=running or not valid):
        st.session_state.running = True
        st.session_state.last_run = 0
        st.rerun()
    if b.button("Arrêter", disabled=not running):
        st.session_state.running = False
        st.rerun()


@st.fragment(run_every=1 if st.session_state.running else None)
def progress():
    if (
        st.session_state.running
        and time.monotonic() - st.session_state.last_run >= interval
    ):
        produce()
        if not st.session_state.running:
            st.rerun()
    if st.session_state.error:
        st.error("Le lot n’a pas pu être terminé : " + st.session_state.error)
    st.subheader("Lots déposés")
    if st.session_state.history:
        st.dataframe(
            st.session_state.history, hide_index=True, use_container_width=True
        )
    else:
        st.write("Aucun lot déposé pendant cette session.")
    if st.session_state.running:
        st.success("Génération continue active")


progress()

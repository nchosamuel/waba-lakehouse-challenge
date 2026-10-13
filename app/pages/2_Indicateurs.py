import streamlit as st
from waba.analytics import TABLES, query, display_rows
from waba.generator import COUNTRIES

st.set_page_config(page_title="WABA · Indicateurs", layout="wide")
st.title("Indicateurs du groupe")
st.caption("Résultats calculés depuis les tables Silver et Gold, accessibles par pays.")
countries = st.multiselect("Pays", COUNTRIES, default=list(COUNTRIES))
label = st.selectbox("Indicateur", list(TABLES))
st.info(
    "Les taux de change et le scénario comptable sont simulés. Les seuils de vigilance suivent le sujet du challenge."
)
if label in ("Sinistres / primes", "Délais des sinistres"):
    st.caption(
        "Les opérations historiques sans contexte comptable sont exclues. Consulte la couverture comptable assurance pour voir ces exclusions."
    )
if label == "Revenu par client":
    st.caption(
        "Revenus observés : commissions et intérêts documentés. La colonne des opérations sans intérêts renseignés indique la couverture manquante."
    )
if countries:
    try:
        data = query(TABLES[label], countries)
        st.dataframe(display_rows(data), use_container_width=True, hide_index=True)
        st.caption(
            f"{len(data)} lignes affichées · maximum 500. Les montants sont en EUR et les ratios sont affichés en pourcentage."
        )
    except Exception as exc:
        st.error(f"Indicateurs indisponibles : {exc}")
else:
    st.warning("Choisis au moins un pays.")
st.link_button("Ouvrir le suivi des traitements", "http://localhost:8081")

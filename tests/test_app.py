"""Functional UI checks against the real local MinIO."""

import unittest
from datetime import date
from streamlit.testing.v1 import AppTest


class AppTests(unittest.TestCase):
    def test_one_time_and_invalid_selection(self):
        app = AppTest.from_file("app/main.py", default_timeout=20).run()
        self.assertFalse(app.exception)
        app.multiselect[0].set_value([]).run()
        self.assertTrue(app.button[0].disabled)
        app.multiselect[0].set_value(["SN"]).run()
        app.date_input[0].set_value(date(2026, 1, 3)).run()
        app.date_input[1].set_value(date(2026, 1, 3)).run()
        app.number_input[0].set_value(10).run()
        app.button[0].click().run()
        self.assertFalse(app.exception)
        self.assertFalse(app.error)
        self.assertEqual(app.session_state["history"][0]["Lignes"], 10)

    def test_continuous_start_tick_stop(self):
        app = AppTest.from_file("app/main.py", default_timeout=20).run()
        app.multiselect[0].set_value(["SN"]).run()
        app.date_input[0].set_value(date(2026, 1, 3)).run()
        app.date_input[1].set_value(date(2026, 1, 3)).run()
        app.number_input[0].set_value(5).run()
        app.radio[0].set_value("Continue").run()
        app.button[0].click().run()
        self.assertTrue(app.session_state["running"])
        self.assertEqual(len(app.session_state["history"]), 1)
        app.session_state["last_run"] = 0
        app.run()
        self.assertEqual(len(app.session_state["history"]), 2)
        app.button[1].click().run()
        self.assertFalse(app.session_state["running"])
        self.assertFalse(app.exception)

    def test_each_new_dataset_and_references(self):
        for label in (
            "Opérations d’assurance",
            "Paiements mobile money",
            "Remboursements de crédit",
            "Référentiels",
        ):
            app = AppTest.from_file("app/main.py", default_timeout=20).run()
            app.selectbox[0].set_value(label).run()
            app.multiselect[0].set_value(["TG"]).run()
            app.date_input[0].set_value(date(2026, 1, 5)).run()
            app.date_input[1].set_value(date(2026, 1, 5)).run()
            app.number_input[0].set_value(2).run()
            app.button[0].click().run()
            self.assertFalse(app.exception)
            self.assertFalse(app.error)
            self.assertEqual(app.session_state["history"][0]["Type"], label)
            self.assertGreater(app.session_state["history"][0]["Lignes"], 0)

    def test_full_reference_profile(self):
        app = AppTest.from_file("app/main.py", default_timeout=30).run()
        app.selectbox[0].set_value("Référentiels").run()
        app.checkbox[0].check().run()
        app.button[0].click().run()
        self.assertFalse(app.exception)
        self.assertFalse(app.error)
        self.assertEqual(app.session_state["history"][0]["Lignes"], 1300250)

    def test_microfinance_entity_can_be_selected(self):
        app = AppTest.from_file("app/main.py", default_timeout=20).run()
        app.selectbox[1].set_value("MICROFINANCE").run()
        app.number_input[0].set_value(3).run()
        app.date_input[0].set_value(date(2026, 1, 6)).run()
        app.date_input[1].set_value(date(2026, 1, 6)).run()
        app.button[0].click().run()
        self.assertFalse(app.exception)
        self.assertFalse(app.error)
        self.assertEqual(
            app.session_state["history"][0]["Ligne métier"], "Microfinance"
        )
        self.assertEqual(app.session_state["history"][0]["Lignes"], 3)

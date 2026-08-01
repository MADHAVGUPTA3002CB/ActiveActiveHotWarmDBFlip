import unittest
from unittest.mock import patch
from unittest.mock import MagicMock

from flipbench.core import build_manifest

from flipbench.recovery import CatalogLeafState, RecoveryError, recovery_steps


class RecoveryPlanTests(unittest.TestCase):
    def test_recovery_plan_is_complete_and_deterministic(self) -> None:
        self.assertEqual(recovery_steps(CatalogLeafState.ATTACHED), ())
        self.assertEqual(
            recovery_steps(CatalogLeafState.PENDING_FINALIZE),
            ("finalize", "attach", "verify"),
        )
        self.assertEqual(
            recovery_steps(CatalogLeafState.DETACHED),
            ("attach", "verify"),
        )

    def test_unknown_catalog_state_fails_closed(self) -> None:
        with self.assertRaises(RecoveryError):
            recovery_steps("unknown")  # type: ignore[arg-type]

    def test_revert_rejects_non_retiring_manifest_before_database_io(self) -> None:
        from flipbench.recovery import RecoveryError, revert_to_hot

        with self.assertRaises(RecoveryError):
            revert_to_hot(None, None, build_manifest(5, "cell01", "active"), 1, 1.0)

    def test_revert_refuses_hot_ddl_after_warm_primary_is_durable(self) -> None:
        from flipbench.recovery import revert_to_hot

        hot = MagicMock()
        warm = MagicMock()
        warm.execute.return_value.fetchone.return_value = ("warm_primary", "warm_primary", 7)

        with self.assertRaisesRegex(RecoveryError, "not recoverable"):
            revert_to_hot(hot, warm, build_manifest(5, "cell01", "retiring"), 7, 1.0)

        hot.execute.assert_not_called()

    def test_remaining_recovery_budget_fails_closed_at_deadline(self) -> None:
        from flipbench.recovery import _remaining_ms

        with patch("flipbench.recovery.time.monotonic", return_value=10.0):
            self.assertEqual(_remaining_ms(10.001), 1)
            with self.assertRaisesRegex(RecoveryError, "deadline expired"):
                _remaining_ms(10.0)


if __name__ == "__main__":
    unittest.main()

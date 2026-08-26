import os
import unittest

from insidertrader.metric_sdk import MetricDescriptor, MetricError, MetricOutput, validate_output
from insidertrader.strategy_sdk import Action, Proposal, ProposalError, validate_proposal
from insidertrader.worker_sandbox import bounded_env_int, network_enabled


class WorkerContractTest(unittest.TestCase):
    def test_metric_and_proposal_contracts_accept_bounded_wire_objects(self) -> None:
        descriptor = MetricDescriptor("momentum.v1", ("return",), 1_000, -1.0, 1.0)
        output = MetricOutput("momentum.v1", 7, 10, 1_000, 0.25, 0.8, 0.1)
        self.assertEqual(validate_output(output, descriptor).to_wire()["instrument_id"], "7")

        proposal = Proposal(1, "momentum.v1", 7, Action("increase", 2), 0.8, 100, 10, 10)
        self.assertEqual(validate_proposal(proposal, 11).to_wire()["action"], {"type": "increase", "value": 2})

    def test_contracts_reject_expired_or_out_of_range_values(self) -> None:
        descriptor = MetricDescriptor("momentum.v1", (), 1_000, -1.0, 1.0)
        with self.assertRaises(MetricError):
            validate_output(MetricOutput("momentum.v1", 7, 0, 1_000, 2.0, 1.0, 0.0), descriptor)
        proposal = Proposal(1, "momentum.v1", 7, Action("increase", 2), 0.8, 100, 10, 10)
        with self.assertRaises(ProposalError):
            validate_proposal(proposal, 20)

    def test_worker_resource_settings_are_bounded_without_silent_coercion(self) -> None:
        name = "IT_TEST_LIMIT"
        previous = os.environ.get(name)
        try:
            os.environ.pop(name, None)
            self.assertEqual(bounded_env_int(name, 10, 1, 100), 10)
            os.environ[name] = "not-an-integer"
            with self.assertRaises(ValueError):
                bounded_env_int(name, 10, 1, 100)
            os.environ[name] = "101"
            with self.assertRaises(ValueError):
                bounded_env_int(name, 10, 1, 100)
        finally:
            if previous is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = previous

    def test_worker_network_policy_is_exact_opt_in(self) -> None:
        name = "IT_PYTHON_ALLOW_NETWORK"
        previous = os.environ.get(name)
        try:
            os.environ.pop(name, None)
            self.assertFalse(network_enabled())
            os.environ[name] = "TRUE"
            self.assertFalse(network_enabled())
            os.environ[name] = "true"
            self.assertTrue(network_enabled())
        finally:
            if previous is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = previous


if __name__ == "__main__":
    unittest.main()

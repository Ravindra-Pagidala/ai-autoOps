from __future__ import annotations

import sys
import os
import time
import argparse
from dataclasses import dataclass
from typing import Callable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agent"))

import structlog
from chaos.models import IncidentEvent, IncidentSeverity
from chaos.inject_pod_crash import inject_pod_crash, inject_pod_crash_loop
from chaos.inject_redis_oom import inject_redis_oom
from chaos.inject_queue_backlog import inject_queue_backlog
from chaos.inject_db_saturation import inject_db_saturation
from chaos.kafka_publisher import close_producer

__all__ = ["run_scenario", "SCENARIOS"]

logger = structlog.get_logger(__name__)


@dataclass
class ScenarioStep:
    """One step in a chaos scenario."""
    name: str
    fn: Callable[[], IncidentEvent]
    wait_after_seconds: float = 5.0
    description: str = ""


@dataclass
class Scenario:
    """A named chaos scenario made up of one or more steps."""
    name: str
    description: str
    steps: list[ScenarioStep]


# ── Scenario Definitions ───────────────────────────────────────────────────────
# Each scenario tests a different failure pattern.
# run all of them to get 100+ parameterized test runs.

SCENARIOS: dict[str, Scenario] = {

    "single_pod_crash": Scenario(
        name="single_pod_crash",
        description="One pod crashes. Agent should detect, confirm K8s recovery, log resolved.",
        steps=[
            ScenarioStep(
                name="crash_one_pod",
                fn=lambda: inject_pod_crash("ticket-service", IncidentSeverity.HIGH, pod_index=0),
                wait_after_seconds=5,
                description="Kill pod 0",
            ),
        ],
    ),

    "multi_pod_crash": Scenario(
        name="multi_pod_crash",
        description="Two pods crash in quick succession. Tests agent handling of rapid failures.",
        steps=[
            ScenarioStep(
                name="crash_pod_0",
                fn=lambda: inject_pod_crash("ticket-service", IncidentSeverity.HIGH, pod_index=0),
                wait_after_seconds=3,
            ),
            ScenarioStep(
                name="crash_pod_1",
                fn=lambda: inject_pod_crash("ticket-service", IncidentSeverity.HIGH, pod_index=1),
                wait_after_seconds=5,
            ),
        ],
    ),

    "crash_loop": Scenario(
        name="crash_loop",
        description="Pod crash loop simulation. Agent should identify pattern and escalate.",
        steps=[
            ScenarioStep(
                name="crash_loop",
                fn=lambda: inject_pod_crash_loop(
                    "ticket-service", IncidentSeverity.CRITICAL, crash_count=3
                ),
                wait_after_seconds=10,
            ),
        ],
    ),

    "redis_oom": Scenario(
        name="redis_oom",
        description="Redis memory exhaustion. Agent should flush chaos keys and verify recovery.",
        steps=[
            ScenarioStep(
                name="flood_redis",
                fn=lambda: inject_redis_oom(IncidentSeverity.HIGH, fill_mb=30),
                wait_after_seconds=5,
            ),
        ],
    ),

    "queue_backlog": Scenario(
        name="queue_backlog",
        description="Kafka queue backlog. Agent should detect lag and recommend scaling consumers.",
        steps=[
            ScenarioStep(
                name="flood_queue",
                fn=lambda: inject_queue_backlog(
                    target_topic="incidents", message_count=5000, severity=IncidentSeverity.HIGH
                ),
                wait_after_seconds=5,
            ),
        ],
    ),

    "db_saturation": Scenario(
        name="db_saturation",
        description="DB connection pool exhaustion. Agent should detect and recommend action.",
        steps=[
            ScenarioStep(
                name="saturate_db",
                fn=lambda: inject_db_saturation(IncidentSeverity.HIGH, connection_count=15, hold_seconds=20),
                wait_after_seconds=5,
            ),
        ],
    ),

    "cascading_failure": Scenario(
        name="cascading_failure",
        description=(
            "Full cascading failure: pod crash → Redis OOM → queue backlog. "
            "Tests agent's ability to handle multiple concurrent incidents. "
            "This is the most complex scenario."
        ),
        steps=[
            ScenarioStep(
                name="crash_pod",
                fn=lambda: inject_pod_crash("ticket-service", IncidentSeverity.CRITICAL, pod_index=0),
                wait_after_seconds=3,
                description="Step 1: crash a pod to start the cascade",
            ),
            ScenarioStep(
                name="oom_redis",
                fn=lambda: inject_redis_oom(IncidentSeverity.HIGH, fill_mb=20),
                wait_after_seconds=3,
                description="Step 2: flood Redis while pod is recovering",
            ),
            ScenarioStep(
                name="backlog_queue",
                fn=lambda: inject_queue_backlog(
                    target_topic="incidents",
                    message_count=3000,
                    severity=IncidentSeverity.HIGH,
                ),
                wait_after_seconds=5,
                description="Step 3: flood queue while Redis is under pressure",
            ),
        ],
    ),

}


def run_scenario(scenario_name: str, dry_run: bool = False) -> None:
    """
    Executes a named chaos scenario step by step.
    dry_run: prints what would happen without actually injecting failures.
    """
    if scenario_name not in SCENARIOS:
        available = ", ".join(SCENARIOS.keys())
        raise ValueError(
            f"Unknown scenario: '{scenario_name}'. Available: {available}"
        )

    scenario = SCENARIOS[scenario_name]
    log = logger.bind(scenario=scenario_name, steps=len(scenario.steps))
    log.info("scenario_start", description=scenario.description)

    for i, step in enumerate(scenario.steps, start=1):
        log.info(
            "scenario_step_start",
            step=i,
            total=len(scenario.steps),
            step_name=step.name,
            description=step.description,
        )

        if dry_run:
            log.info("scenario_step_dry_run", step_name=step.name)
        else:
            try:
                event = step.fn()
                log.info(
                    "scenario_step_complete",
                    step=i,
                    step_name=step.name,
                    incident_id=event.incident_id,
                    incident_type=event.incident_type,
                )
            except Exception as e:
                log.error(
                    "scenario_step_failed",
                    step=i,
                    step_name=step.name,
                    error=str(e),
                )
                raise

        if i < len(scenario.steps):
            log.info(
                "scenario_step_waiting",
                seconds=step.wait_after_seconds,
            )
            if not dry_run:
                time.sleep(step.wait_after_seconds)

    log.info("scenario_complete", scenario=scenario_name)


def list_scenarios() -> None:
    """Prints all available scenarios with descriptions."""
    print("\nAvailable chaos scenarios:\n")
    for name, scenario in SCENARIOS.items():
        print(f"  {name}")
        print(f"    {scenario.description}")
        print(f"    Steps: {len(scenario.steps)}")
        print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run chaos scenarios")
    parser.add_argument("--scenario", help="Scenario name to run")
    parser.add_argument("--list", action="store_true", help="List all scenarios")
    parser.add_argument("--dry-run", action="store_true", help="Print without executing")
    args = parser.parse_args()

    if args.list:
        list_scenarios()
        sys.exit(0)

    if not args.scenario:
        parser.print_help()
        sys.exit(1)

    try:
        run_scenario(args.scenario, dry_run=args.dry_run)
    finally:
        close_producer()
from datetime import timedelta
from typing import Optional
from pendulum import Date, DateTime, Time, timezone

from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

UTC = timezone("UTC")


class EndOfMonthTimetable(Timetable):

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        # Is day last of month?
        days_until_end_of_month = run_after.end_of('month').day - run_after.day
        if days_until_end_of_month == 0: # If it is the last day of the month, data interval is the current month
            start = run_after.start_of('month').replace(tzinfo=UTC)
            end = run_after.end_of('month').replace(tzinfo=UTC)
        else: # If it is not the last day of the month, data interval is the previous month
            start = run_after.subtract(months=1).start_of('month').replace(tzinfo=UTC)
            end = run_after.subtract(months=1).end_of('month').replace(tzinfo=UTC)
        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_data_interval is not None:  # There was a previous run on the regular schedule.
            last_start = last_automated_data_interval.start
            next_start = last_start.start_of('month').add(months=1).replace(tzinfo=UTC)
            next_end = next_start.end_of('month')
        else:  # This is the first ever run on the regular schedule.
            next_start = restriction.earliest
            if next_start is None:  # No start_date. Don't schedule.
                return None
            if not restriction.catchup: # If the DAG has catchup=False, today is the earliest to consider.
                next_start = max(next_start, DateTime.combine(Date.today(), Time.min).replace(tzinfo=UTC))
            if next_start.day != 1: # If the DAG's start date is not the first of the month, run for the next month so data_interval_start is not > start_date
                next_start = next_start.start_of('month').add(months=1).replace(tzinfo=UTC)
                next_end = next_start.end_of('month').replace(tzinfo=UTC)
            else:  # If the DAG's start date is the first of the month, run for that month
                next_start = next_start.start_of('month').replace(tzinfo=UTC)
                next_end = next_start.end_of('month').replace(tzinfo=UTC)
        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        return DagRunInfo.interval(start=next_start, end=next_end)


class EndOfMonthTimetablePlugin(AirflowPlugin):
    name = "end_of_month_timetable_plugin"
    timetables = [EndOfMonthTimetable]

# airflow-scheduling-tutorial

This repo contains an example Airflow project for implementing custom timetables with Airflow 2.2. A full guide walking through the timetables concept will be published shortly.

There are two example timetables in the `plugins/` directory of this repo:

- **UnevenIntervalsTimetable**: schedules the DAG at two times during the day with uneven intervals, in this example 6:00 and 16:30. This is not possible using a cron expression.
- **EndOfMonthTimetable**: schedules the DAG on the last day of the month.

The `example-dag` shows implementation of the `UnevenIntervalsTimetable`.

## Getting Started
The easiest way to run these example timetables is to use the Astronomer CLI to get an Airflow instance up and running locally. Note that you must be using Airflow 2.2!

 1. [Install the Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart)
 2. Clone this repo somewhere locally and navigate to it in your terminal
 3. Initialize an Astronomer project by running `astro dev init`
 4. Start Airflow locally by running `astro dev start`
 5. Navigate to localhost:8080 in your browser and you should see the tutorial DAGs there

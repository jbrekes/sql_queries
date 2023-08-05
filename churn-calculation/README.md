# Day-7 Churn Calculation in Games

This repository contains a Databricks notebook that calculates Day-7 churn for specific games. The notebook extracts and processes game event data to determine whether players have churned (not played for 7 or more days) and aggregates the results for analysis.

## Tables Used

1. `round_event_raw`: Contains data related to the rounds played in each game. This table provides information about the players, rounds, applications, and more.

2. `room_lkp`: A lookup table that contains all available rooms (levels) designed for each game. This table provides additional information about the rooms, such as names and locations.

## How It Works

1. The notebook begins by creating a temporary view named `dateToProcess` using a widget to set the processing date.

2. It extracts all rounds played in the last 21 days and sorts them from most recent to oldest, creating a temporary view named `v0_rounds_raw`.

3. The notebook creates a series of temporary views (`v0_last_activity`, `v1_last_activity`, `v0_d7_churned`) to perform various data transformations, including identifying the first round played each day, calculating days between rounds, and determining churn status.

4. Data is aggregated and summarized into a temporary view named `churn_final` for easier analysis and export.

5. The data in the `churn_final` view is written to a Delta table.

6. The notebook executes control queries to manage the Delta table.

## How to Use

1. Set the processing date using the widget (optional).

2. Run each cell in the notebook sequentially.

3. Review and analyze the results in the Delta table or perform further analysis using SQL queries.

## Prerequisites

- Databricks environment
- Access to relevant game event data
- Basic understanding of SQL and Databricks notebooks

## Contributing

Contributions to this project are welcome. Feel free to submit pull requests or open issues for any enhancements or bug fixes.

## License

This project is licensed under the [MIT License](LICENSE).

---

Please note that you might need to adapt the content to match your specific use case, organization, and style preferences.


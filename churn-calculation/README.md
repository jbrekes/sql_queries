# 🎮 Day-7 Churn Calculation in Games

This repository contains a Databricks notebook that demonstrates advanced SQL skills by calculating Day-7 churn for specific games. The notebook extracts and processes game event data to determine whether players have churned (not played for 7 or more days) and aggregates the results for analysis.

## 📊 Tables Used

1. `round_event_raw`: Contains data related to the rounds played in each game. This table provides information about the players, rounds, applications, and more.

2. `room_lkp`: A lookup table that contains all available rooms (levels) designed for each game. This table provides additional information about the rooms, such as names and locations.

## 🌟 Highlights and Tools

- **Widget Usage**: The notebook showcases the use of Databricks widgets to dynamically set the processing date, offering flexibility in analyzing different time periods.

- **Common Table Expressions (CTEs)**: The SQL code employs CTEs to break down complex logic into manageable steps, enhancing code readability and maintainability.

- **Window Functions**: The use of window functions, such as `ROW_NUMBER()` and `LEAD()`, enables advanced data manipulations and temporal analysis.

- **Conditional Logic**: Conditional statements and calculations, including `CASE` expressions, are used to categorize and derive meaningful insights from the data.

- **Delta Tables**: The notebook demonstrates the use of Delta tables to efficiently store and manage the churn analysis results, allowing for faster queries and optimizations.

## 💡 How It Works

1. The notebook begins by creating a temporary view named `dateToProcess` using a widget to set the processing date.

2. It extracts all rounds played in the last 21 days and sorts them from most recent to oldest, creating a temporary view named `v0_rounds_raw`.

3. The notebook creates a series of temporary views (`v0_last_activity`, `v1_last_activity`, `v0_d7_churned`) to perform various data transformations, including identifying the first round played each day, calculating days between rounds, and determining churn status.

4. Data is aggregated and summarized into a temporary view named `churn_final` for easier analysis and export.

5. The data in the `churn_final` view is written to a Delta table.

6. The notebook executes control queries to manage the Delta table.

## 🚀 How to Use

1. Clone this repository to your Databricks environment.
2. Open the notebook `Churn_Analysis.ipynb` in your Databricks workspace.
3. Set the processing date using the widget if needed.
4. Execute each cell in sequence to perform the churn analysis.
5. Explore the results in the generated Delta table.
6. Customize the notebook or queries to match your specific game data and requirements.

## 🤝 Contributing

We welcome contributions from the community to enhance and improve this churn analysis notebook. If you find a bug, have a suggestion, or want to add a new feature, please feel free to open an issue or submit a pull request.

## 📜 License

This project is licensed under the [MIT License](LICENSE).

---

Feel free to modify the content to match your specific use case, organization, and style preferences.



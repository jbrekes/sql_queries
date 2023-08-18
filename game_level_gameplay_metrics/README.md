# 📊 Gameplay Metrics Analysis

Welcome to a data analytics portfolio showcasing a Databricks notebook designed to analyze gameplay metrics in various games. This project delves into virtual item usage and rounds played data to extract insights into player behavior and gaming patterns.

## Table of Contents

- [Introduction](#introduction)
- [🌟 Highlights](#highlights)
- [📔 Notebook Sections](#notebook-sections)

## Introduction

This notebook aims to provide a comprehensive analysis of gameplay metrics by processing data from diverse games. The goal is to uncover trends, understand player engagement, and shed light on gaming strategies. The notebook demonstrates the ability to manipulate complex datasets and derive actionable insights.

## 🌟 Highlights

### Complex Data Transformation 🔄

One of the notable challenges of this project is the intricate data transformation. The code standardizes virtual item classifications across various games, addressing varying nomenclatures and rules. The `v0_virtual_item_full` and `v1_virtual_item_full` views exemplify this complexity, showcasing the capability to structure and unify diverse data sources.

### Virtual Currency Usage Analysis 💰

Analyzing virtual currency usage involves handling complex in-game actions, such as purchasing and using items. The `vo_game_vc_usage` view captures these actions, highlighting the analytical skills in dissecting player interactions and understanding virtual economies.

### Rounds Played Analysis 🎮

The `vo_rounds_order` view provides in-depth analysis of gameplay data, encompassing elements like rounds, rooms, moves, and outcomes. Calculating averages, managing various game mechanics, and filtering outliers demonstrate expertise in extracting meaningful insights from raw data.

### Session and Round Relationships 🔗

Continuing from the previous analysis, the notebook establishes relationships between sessions and rounds. This offers deeper insights into user behavior across multiple rounds and gameplay sessions.

## 📔 Notebook Sections

The notebook is structured into several key sections:

1. **Widget Definition**: Defines the `dateToProcess` widget for inputting the processing date.
2. **📝 Notebook Metadata and Documentation**: Provides a high-level overview and documentation links.
3. **🎮 View Creation for Date Selection and Game Filtering**: Sets up views for processing date and game filtering.
4. **🔍 Virtual Item Classification**: Unifies virtual item classifications, showcasing the ability to transform diverse data.
5. **💰 Virtual Currency Usage Analysis**: Analyzes complex in-game actions to understand virtual economies.
6. **🎮 Rounds Played Analysis**: Explores gameplay data, demonstrating expertise in uncovering insights.
7. **🔗 Session and Round Relationships**: Establishes links for a deeper understanding of user behavior.
8. **📊 Combine Results**: Combines analysis results for a comprehensive overview.

## Requirements

- A Databricks or compatible environment for running SQL code.
- Access to the relevant gameplay metrics dataset.
- Basic understanding of SQL queries and Databricks notebooks.

## Contributing

Contributions are welcome! If you identify issues or opportunities for enhancement, feel free to open a pull request.

## License

This project is licensed under the [MIT License](LICENSE).


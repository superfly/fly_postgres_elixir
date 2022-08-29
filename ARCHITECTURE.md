# Architecture Overview

This explains how the library works and documents decisions and thought processes.


Mermaid diagram?
PNG diagram?

## Tracker

- Owns two ETS tables.
- The GenServer sets up the tables but has no behavior itself.
- The module defines functions for accessing the ETS table data.
- Other processes manage reading, writing, and deleting entries from the ETS table. This prevents any potential bugs in the code from crashing the Tracker process and losing all the ETS table data.

## Reader

- Executes the stored procedure
- After updates are found, writes the updates to the LSN ETS table and executes a function defined in the Tracker module for notifying subscribed/requesting processes.
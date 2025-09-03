import pandas as pd
import matplotlib.pyplot as plt
import os
import glob
import numpy as np
from scipy.stats import gmean


def plot_query_performance(csv_file, time_col="Med_ms", save_plots=True):
    try:
        # Load CSV
        df = pd.read_csv(csv_file)

        # Check if required columns exist
        required_cols = ["DB", "rows", time_col]
        if not all(col in df.columns for col in required_cols):
            print(f"Skipping {csv_file}: Missing required columns {required_cols}")
            return

        # Pivot for easier plotting
        pivot_df = df.pivot_table(
            index=["DB", "rows"], values=time_col, aggfunc="mean"
        ).reset_index()

        # Define preferred DB order, but only use DBs that exist in data
        preferred_order = ["Postgres", "SQLite", "HSQLDB"]
        available_dbs = pivot_df["DB"].unique()
        dbs = [db for db in preferred_order if db in available_dbs]

        # Add any remaining DBs not in preferred order
        dbs.extend([db for db in available_dbs if db not in dbs])

        # Reorder DBs
        pivot_df["DB"] = pd.Categorical(pivot_df["DB"], categories=dbs, ordered=True)
        pivot_df = pivot_df.sort_values(["DB", "rows"])

        # Get unique row counts
        rows = sorted(pivot_df["rows"].unique())

        # Plot grouped bar chart
        fig, ax = plt.subplots(figsize=(12, 6))
        bar_width = 0.35 if len(rows) <= 2 else 0.2
        x = range(len(dbs))

        for i, row in enumerate(rows):
            values = []
            for db in dbs:
                db_row_data = pivot_df[
                    (pivot_df["DB"] == db) & (pivot_df["rows"] == row)
                ]
                if len(db_row_data) > 0:
                    values.append(db_row_data[time_col].values[0])
                else:
                    values.append(0)  # Skip missing data

            ax.bar(
                [pos + i * bar_width for pos in x],
                values,
                width=bar_width,
                label=f"{row:,} rows",
            )

        # Formatting
        ax.set_xticks([pos + (len(rows) - 1) / 2 * bar_width for pos in x])
        ax.set_xticklabels(dbs)
        ax.set_ylabel(f"{time_col} (ms)")
        ax.set_xlabel("Database")

        # Create title from filename and query info
        base_name = os.path.splitext(os.path.basename(csv_file))[0]
        query_name = df["query"].iloc[0] if "query" in df.columns else base_name
        ax.set_title(f"Query Performance: {query_name}")

        ax.legend(title="Dataset Size", bbox_to_anchor=(1.05, 1), loc="upper left")
        ax.grid(True, alpha=0.3)

        # Add value labels on bars
        for i, row in enumerate(rows):
            values = []
            for db in dbs:
                db_row_data = pivot_df[
                    (pivot_df["DB"] == db) & (pivot_df["rows"] == row)
                ]
                if len(db_row_data) > 0:
                    values.append(db_row_data[time_col].values[0])
                else:
                    values.append(0)

            max_val = (
                max([v for v in values if v > 0]) if any(v > 0 for v in values) else 1
            )
            for j, v in enumerate(values):
                if v > 0:
                    ax.text(
                        j + i * bar_width,
                        v + max_val * 0.02,
                        f"{v:.0f}",
                        ha="center",
                        va="bottom",
                        fontsize=8,
                        rotation=0,
                    )

        plt.tight_layout()

        if save_plots:
            # Save plot as PNG
            output_name = f"{base_name}_performance.png"
            plt.savefig(output_name, dpi=300, bbox_inches="tight")
            print(f"Saved plot: {output_name}")
            # Close the figure to prevent it from showing and free memory
            plt.close(fig)
        else:
            # Only show if not saving
            plt.show()

    except Exception as e:
        print(f"Error processing {csv_file}: {str(e)}")


def plot_geometric_mean_performance(time_col="Med_ms", save_plots=True):
    """Generate a geometric mean performance diagram across all queries that ran on all three databases"""
    try:
        # Find all CSV files
        csv_files = glob.glob("*.csv")
        if not csv_files:
            print("No CSV files found for geometric mean calculation.")
            return

        all_data = []
        target_dbs = {"Postgres", "SQLite", "HSQLDB"}

        # Collect data from all CSV files
        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                required_cols = ["DB", "rows", time_col]

                if not all(col in df.columns for col in required_cols):
                    continue

                # Group by query and rows to check if all three databases are present
                for (rows, query), group in df.groupby(["rows", "query"]):
                    available_dbs = set(group["DB"].unique())

                    # Only include if all three target databases are present
                    if target_dbs.issubset(available_dbs):
                        for _, row in group.iterrows():
                            if (
                                row["DB"] in target_dbs and row[time_col] > 0
                            ):  # Exclude zero/negative values
                                all_data.append(
                                    {
                                        "DB": row["DB"],
                                        "rows": rows,
                                        "query": query,
                                        "time": row[time_col],
                                        "file": csv_file,
                                    }
                                )

            except Exception as e:
                print(f"Error reading {csv_file}: {e}")
                continue

        if not all_data:
            print(
                "No data found where all three databases (Postgres, SQLite, HSQLDB) ran the same queries."
            )
            return

        # Convert to DataFrame
        combined_df = pd.DataFrame(all_data)

        print(
            f"Found {len(combined_df)} qualifying data points across {len(combined_df.groupby(['query', 'rows']))} query/dataset combinations"
        )

        # Get unique databases and row counts
        db_order = ["Postgres", "SQLite", "HSQLDB"]
        available_dbs = [db for db in db_order if db in combined_df["DB"].unique()]
        rows = sorted(combined_df["rows"].unique())

        # Calculate geometric mean for each database and row count combination
        geometric_means = {}
        for db in available_dbs:
            geometric_means[db] = {}
            for row_count in rows:
                db_row_data = combined_df[
                    (combined_df["DB"] == db) & (combined_df["rows"] == row_count)
                ]["time"].values
                if len(db_row_data) > 0:
                    # Use scipy's geometric mean
                    geometric_means[db][row_count] = gmean(db_row_data)
                else:
                    geometric_means[db][row_count] = 0

        # Create the plot (same style as other diagrams)
        fig, ax = plt.subplots(figsize=(12, 6))
        bar_width = 0.35 if len(rows) <= 2 else 0.2
        x = range(len(available_dbs))

        for i, row_count in enumerate(rows):
            values = []
            for db in available_dbs:
                values.append(geometric_means[db][row_count])

            ax.bar(
                [pos + i * bar_width for pos in x],
                values,
                width=bar_width,
                label=f"{row_count:,} rows",
            )

        # Formatting (same as other diagrams)
        ax.set_xticks([pos + (len(rows) - 1) / 2 * bar_width for pos in x])
        ax.set_xticklabels(available_dbs)
        ax.set_ylabel(f"Geometric Mean {time_col} (ms)")
        ax.set_xlabel("Database")
        ax.set_title(
            "Overall Performance: Geometric Mean Across All Comparable Queries"
        )
        ax.legend(title="Dataset Size", bbox_to_anchor=(1.05, 1), loc="upper left")
        ax.grid(True, alpha=0.3)

        # Add value labels on bars
        for i, row_count in enumerate(rows):
            values = []
            for db in available_dbs:
                values.append(geometric_means[db][row_count])

            max_val = (
                max([v for v in values if v > 0]) if any(v > 0 for v in values) else 1
            )
            for j, v in enumerate(values):
                if v > 0:
                    ax.text(
                        j + i * bar_width,
                        v + max_val * 0.02,
                        f"{v:.1f}",
                        ha="center",
                        va="bottom",
                        fontsize=8,
                        rotation=0,
                    )

        total_queries = len(combined_df["query"].unique())
        ax.text(
            0.01,
            0.98,
            f"Based on {total_queries} queries",
            transform=ax.transAxes,
            verticalalignment="top",
            bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5),
        )

        plt.tight_layout()

        if save_plots:
            output_name = "geometric_mean_performance.png"
            plt.savefig(output_name, dpi=300, bbox_inches="tight")
            print(f"Saved geometric mean plot: {output_name}")
            plt.close(fig)
        else:
            plt.show()

        # Print summary
        print(f"\nGeometric Mean Performance Summary:")
        print(f"{'Database':<12} ", end="")
        for row_count in rows:
            print(f"{row_count:>12,} rows", end="")
        print()
        print("-" * (12 + len(rows) * 17))

        for db in available_dbs:
            print(f"{db:<12} ", end="")
            for row_count in rows:
                mean_val = geometric_means[db][row_count]
                print(f"{mean_val:>17.1f}", end="")
            print()

    except Exception as e:
        print(f"Error generating geometric mean plot: {str(e)}")


def process_all_csvs(time_col="Med_ms", save_plots=True):
    """Process all CSV files in the current directory"""

    # Find all CSV files in current directory
    csv_files = glob.glob("*.csv")

    if not csv_files:
        print("No CSV files found in the current directory.")
        return

    print(f"Found {len(csv_files)} CSV file(s):")
    for file in csv_files:
        print(f"  - {file}")

    print(f"\nProcessing individual files... (save_plots={save_plots})")

    # Process each CSV file
    for csv_file in csv_files:
        print(f"\n{'='*50}")
        print(f"Processing: {csv_file}")
        print("=" * 50)

        plot_query_performance(csv_file, time_col, save_plots)

    # Generate geometric mean diagram
    print(f"\n{'='*60}")
    print("Generating Geometric Mean Performance Diagram")
    print("=" * 60)
    plot_geometric_mean_performance(time_col, save_plots)


# Run for all CSV files in current directory
if __name__ == "__main__":
    # Save plots without showing windows
    process_all_csvs(time_col="Med_ms", save_plots=True)

    # Uncomment this line if you want to display plots without saving
    # process_all_csvs(time_col="Med_ms", save_plots=False)

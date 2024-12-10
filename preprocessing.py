import pandas as pd
import numpy as np
from datetime import date, datetime
import unicodedata

def convert_minutes_to_float(time_str):
    if type(time_str) != str:
        return time_str
    if ":" in time_str:
        minutes, seconds = map(int, time_str.split(":"))
        return minutes + seconds / 60
    else:
        return 0


class Preprocessor:

    def __init__(self, seasons, span, shift, full):
        self.seasons = seasons
        self.games = pd.DataFrame()
        print("Loading games")
        self.load_all_games()
        self.team_stats = pd.DataFrame()
        self.span = span
        self.shift = shift
        self.current = self.shift == 0

        print("Loading team data")
        self.load_team_data()

    def load_all_games(self):
        seasons = []
        for season in self.seasons:
            df_games = pd.read_csv(f"{season}_all_games.csv")
            seasons.append(df_games)
        self.games = pd.concat(seasons)
        self.games["GAME_DATE"] = self.games["GAME_DATE"].apply(
            lambda x: date(*map(int, x.split("-")))
        )

    def load_team_data(self):
        seasons = []
        for season in self.seasons:

            df_games = pd.read_csv(f"{season}_all_games.csv")
            df_advanced = pd.read_csv(f"{season}_advanced_stats.csv")[
                [
                    "gameId",
                    "teamTricode",
                    "estimatedOffensiveRating",
                    "offensiveRating",
                    "estimatedDefensiveRating",
                    "defensiveRating",
                    "estimatedNetRating",
                    "netRating",
                    "assistPercentage",
                    "assistToTurnover",
                    "assistRatio",
                    "offensiveReboundPercentage",
                    "defensiveReboundPercentage",
                    "reboundPercentage",
                    "turnoverRatio",
                    "effectiveFieldGoalPercentage",
                    "trueShootingPercentage",
                    "usagePercentage",
                    "estimatedUsagePercentage",
                    "estimatedPace",
                    "pace",
                    "pacePer40",
                    "possessions",
                    "PIE",
                ]
            ]
            df_basic = pd.read_csv(f"{season}_traditional_stats.csv")[
                [
                    "gameId",
                    "teamTricode",
                    "fieldGoalsMade",
                    "fieldGoalsAttempted",
                    "fieldGoalsPercentage",
                    "threePointersMade",
                    "threePointersAttempted",
                    "threePointersPercentage",
                    "freeThrowsMade",
                    "freeThrowsAttempted",
                    "freeThrowsPercentage",
                    "reboundsOffensive",
                    "reboundsDefensive",
                    "reboundsTotal",
                    "assists",
                    "steals",
                    "blocks",
                    "turnovers",
                    "foulsPersonal",
                    "points",
                    "plusMinusPoints",
                ]
            ]
            df_hustle = pd.read_csv(f"{season}_hustle_stats.csv")[
                [
                    "gameId",
                    "teamTricode",
                    "contestedShots",
                    "contestedShots2pt",
                    "contestedShots3pt",
                    "deflections",
                    "chargesDrawn",
                    "screenAssists",
                    "screenAssistPoints",
                    "looseBallsRecoveredOffensive",
                    "looseBallsRecoveredDefensive",
                    "looseBallsRecoveredTotal",
                    "offensiveBoxOuts",
                    "defensiveBoxOuts",
                    "boxOutPlayerTeamRebounds",
                    "boxOutPlayerRebounds",
                    "boxOuts",
                ]
            ]
            df_misc = pd.read_csv(f"{season}_misc_stats.csv")[
                [
                    "gameId",
                    "teamTricode",
                    "pointsOffTurnovers",
                    "pointsSecondChance",
                    "pointsFastBreak",
                    "pointsPaint",
                    "oppPointsOffTurnovers",
                    "oppPointsSecondChance",
                    "oppPointsFastBreak",
                    "oppPointsPaint",
                    "blocksAgainst",
                    "foulsDrawn",
                ]
            ]
            df_tracking = pd.read_csv(f"{season}_track_stats.csv")[
                [
                    "gameId",
                    "teamTricode",
                    "distance",
                    "reboundChancesOffensive",
                    "reboundChancesDefensive",
                    "reboundChancesTotal",
                    "touches",
                    "secondaryAssists",
                    "freeThrowAssists",
                    "passes",
                    "contestedFieldGoalsMade",
                    "contestedFieldGoalsAttempted",
                    "contestedFieldGoalPercentage",
                    "uncontestedFieldGoalsMade",
                    "uncontestedFieldGoalsAttempted",
                    "uncontestedFieldGoalsPercentage",
                    "defendedAtRimFieldGoalsMade",
                    "defendedAtRimFieldGoalsAttempted",
                    "defendedAtRimFieldGoalPercentage",
                ]
            ]

            merge_columns = ["gameId", "teamTricode"]
            merged_df = pd.merge(df_advanced, df_basic, on=merge_columns, how="inner")
            merged_df = pd.merge(merged_df, df_hustle, on=merge_columns, how="inner")
            merged_df = pd.merge(merged_df, df_misc, on=merge_columns, how="inner")
            merged_df = pd.merge(merged_df, df_tracking, on=merge_columns, how="inner")
            merged_df = merged_df.drop_duplicates()

            merged_df = pd.merge(
                merged_df, df_games[["gameId", "GAME_DATE"]], on="gameId", how="left"
            )
            merged_df["date"] = merged_df["GAME_DATE"].apply(
                lambda x: date(*map(int, x.split("-")))
            )
            merged_df = merged_df.drop(columns=["GAME_DATE"])

            df_games["winner"] = np.where(
                df_games["HOME_TEAM_PTS"] > df_games["AWAY_TEAM_PTS"],
                df_games["HOME_TEAM_ABBREVIATION"],
                df_games["AWAY_TEAM_ABBREVIATION"],
            )

            merged_df = pd.merge(
                merged_df,
                df_games[
                    [
                        "gameId",
                        "winner",
                        "HOME_TEAM_ABBREVIATION",
                        "AWAY_TEAM_ABBREVIATION",
                    ]
                ],
                on="gameId",
                how="left",
            )

            merged_df = merged_df.drop_duplicates(subset=["gameId", "teamTricode"])
            
            processed_df = self.preprocess_team_data(merged_df)

            seasons.append(processed_df)

        self.team_stats = pd.concat(seasons)
        self.team_stats = self.team_stats.drop(columns=["HOME_TEAM_ABBREVIATION", "AWAY_TEAM_ABBREVIATION", "winner"])

    def preprocess_team_data(self, df):

        grouped = df.groupby("teamTricode")
        modified_groups = []
        for name, group in grouped:
            group["game_count"] = (
                group["gameId"].expanding().count().shift(self.shift).fillna(0)
            )
            group["time_between_games"] = group["date"].diff().dt.days
            group["playoff"] = (group["game_count"] > 82).astype(int)

            group = self.generate_team_running_averages(group)

            modified_groups.append(group)

        return pd.concat(modified_groups)

    def generate_team_running_averages(self, group):
        percentage_columns = [
            "assistPercentage",
            "assistToTurnover",
            "assistRatio",
            "offensiveReboundPercentage",
            "defensiveReboundPercentage",
            "reboundPercentage",
            "turnoverRatio",
            "effectiveFieldGoalPercentage",
            "trueShootingPercentage",
            "usagePercentage",
            "estimatedUsagePercentage",
            "fieldGoalsPercentage",
            "threePointersPercentage",
            "freeThrowsPercentage",
            "contestedFieldGoalPercentage",
            "uncontestedFieldGoalsPercentage",
            "defendedAtRimFieldGoalPercentage",
        ]
        averaging_columns = [
            col
            for col in group.columns
            if col
            not in [
                "teamTricode",
                "gameId",
                "date",
                "game_count",
                "time_between_games",
                "playoff",
                "winning_percentage",
                "home_winning_percentage",
                "away_winning_percentage",
                "home_streak",
                "streak",
                "away_streak",
                "win",
                "winner",
                "HOME_TEAM_ABBREVIATION",
                "AWAY_TEAM_ABBREVIATION",
            ]
        ]
        for col in averaging_columns:

            running_avg_col_name = f"running_avg_{col}_last_{self.span}"

            group[running_avg_col_name] = (
                group[col].ewm(span=self.span, min_periods=1).mean().shift(self.shift)
            )
        group = group.drop(columns=averaging_columns)
        return group


if __name__ == "__main__":
    seasons = [
        "2024-25",
        "2023-24"
    ]
    p = Preprocessor(seasons, 50, 1, full=True)
    p25 = Preprocessor(seasons, 25, 1, False)
    p10 = Preprocessor(seasons, 10, 1, False)
    p5 = Preprocessor(seasons, 5, 1, False)
    p3 = Preprocessor(seasons, 3, 1, False)

    common_cols = list(
        set(p.team_stats.columns).intersection(set(p25.team_stats.columns))
    )

    p.team_stats = pd.merge(
        p.team_stats, p25.team_stats, on=common_cols, how="inner"
    )
    p.team_stats = pd.merge(
        p.team_stats, p10.team_stats, on=common_cols, how="inner"
    )

    print("Processing complete, saving data")
    try:
        p.games.to_csv("all_games.csv")
        p.team_stats.to_csv("all_team_averages.csv")
    except PermissionError as e:
        print(f"Caught {e}, saving to backup files")
        p.games.to_csv("backup_all_games.csv")
        p.team_stats.to_csv("backup_all_team_averages")

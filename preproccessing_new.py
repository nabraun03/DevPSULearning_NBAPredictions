import pandas as pd
import numpy as np
from datetime import date

class Preprocessor:
    
    def __init__(self, seasons, span, shift):
        self.seasons = seasons
        self.games = pd.DataFrame()
        print("Loading games")
        self.load_all_games()
        self.team_stats = pd.DataFrame()
        self.span = span
        self.shift = shift

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

        df_traditional = pd.read_csv(f"{season}_traditional_stats.csv")[
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


if __name__ == "__main__":

    seasons = ["2024-25"]

    p50 = Preprocessor(seasons, 50, 1)
    p25 = Preprocessor(seasons, 25, 1)
    p10 = Preprocessor(seasons, 25, 1)
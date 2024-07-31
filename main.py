from pulsefire.clients import RiotAPIClient
import pandas as pd
import os

API_KEY = os.getenv('RIOT_API_KEY')

async def get_division(tier='DIAMOND', division='I', queue='RANKED_SOLO_5x5'):
    async with RiotAPIClient(default_headers={"X-Riot-Token": API_KEY}) as client:
        return await client.get_lol_league_v4_entries_by_division(tier=tier, division=division, queue=queue, region='euw1')

async def get_player_info(summonerId):
    async with RiotAPIClient(default_headers={"X-Riot-Token": API_KEY}) as client:
        return await client.get_lol_summoner_v4_by_id(id=summonerId, region='euw1')

async def get_match_history(summoner_puuid):
    async with RiotAPIClient(default_headers={"X-Riot-Token": API_KEY}) as client:
        return await client.get_lol_match_v5_match_ids_by_puuid(puuid=summoner_puuid, region='europe')

async def get_match_timeline(match_id='EUW1_7044136381'):
    async with RiotAPIClient(default_headers={"X-Riot-Token": API_KEY}) as client:
        return await client.get_lol_match_v5_match_timeline(id=match_id, region='europe')

async def get_match(match_id):
    async with RiotAPIClient(default_headers={"X-Riot-Token": API_KEY}) as client:
        return await client.get_lol_match_v5_match(id=match_id, region='europe')


RELEVANT_COLUMNS = ('side', 'jungleMinionsKilled', 'level', 'minionsKilled', 'totalGold', 'xp', 'totalDamageDoneToChampions', 'totalDamageTaken')

def extract_player_match_data(match, maxFrame=15):
    df = pd.DataFrame.from_dict(match['info']['frames'][maxFrame]['participantFrames'], orient='index')
    df = df.drop(columns=['damageStats']).reset_index().join(pd.json_normalize(df['damageStats'])).set_index('index')
    #df = df.join(pd.json_normalize(df['damageStats']))
    df['side'] = ['blue' if int(idx) > 5 else 'red' for idx in df.index]
    return df[[*RELEVANT_COLUMNS]]


def process_frames(timeline):
    parsed_events = []
    for frame in timeline['info']['frames']:
        events = frame['events']
        for ev in events:
            if ev['type'] not in ('CHAMPION_KILL', 'ELITE_MONSTER_KILL', 'BUILDING_KILL', 'CHAMPION_SPECIAL_KILL'):
                continue

            match ev['type']:
                case 'CHAMPION_KILL':
                    data = {'assists': ev.get('assistingParticipantIds', []), 'killer': ev['killerId']}
                case 'ELITE_MONSTER_KILL':
                    data = {'monster': ev.get('monsterType'), 'team_id': ev['killerTeamId']}
                case 'BUILDING_KILL':
                    data = {'building': ' '.join([ev['buildingType'], ev.get('towerType', '')]), 'lane': ev['laneType'],
                            'team_id': ev['teamId']}
            parsed_events.append(data)

    return parsed_events

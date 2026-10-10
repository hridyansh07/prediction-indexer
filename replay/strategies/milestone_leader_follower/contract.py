"""Closed pins and exact arithmetic for the independent V1 scenario."""
from fractions import Fraction
import re

from replay.economic_sdk.game import game_policy, experiment_policy
from replay.preparation import digest, encoded, sha
from replay.streams.protocol import obj, require, uint
from replay.strategy_sdk import plain

STRATEGY = 'milestone_leader_follower_v1'
ROLES = ('target_only', 'target_milestones', 'leader_milestones')
DECIMAL = re.compile(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?$')


def number(value, *, nonnegative=False):
    require(type(value) is str and len(value) <= 80 and bool(DECIMAL.fullmatch(value)), 'exact canonical decimal')
    require(value != '-0', 'negative zero')
    result = Fraction(value)
    require(not nonnegative or result >= 0, 'nonnegative amount')
    return result


def rational(value):
    require(type(value) is str and len(value) <= 160, 'rational string')
    try:
        result = Fraction(value)
    except (ValueError, ZeroDivisionError) as error:
        raise ValueError('rational string') from error
    require(value == str(result), 'canonical rational')
    return result


def r(value):
    return str(Fraction(value))


def policy(value):
    value = obj(plain(value), 'version game cohort quantity_grid window_ns history_ns history_changes exit_horizon_ns decision_delay_ns minimum_leader_move minimum_forecast_margin minimum_forecast_return_bps take_profit_fraction stop_loss_fraction exit_on_next_segment_end exit_on_match_end cooldown_after_close_ns rearm_false_ns max_entries_per_event max_open_per_target capacity_mode trading_status_mode valuation initial_cash transaction_budget event_budget maximum_outstanding_cost maximum_concurrent_positions holding_rate_per_ns settlement models rule_bindings max_books max_routes models_tried thresholds_tried')
    require(type(value['version']) is int and value['version'] == 1, 'leader policy version')
    value['game'] = game_policy(value['game'])
    require(value['cohort'] in ('milestone_primary', 'book_only_comparison'), 'cohort')
    require(value['game']['required'] or value['cohort'] == 'book_only_comparison', 'primary requires game')
    require(value['cohort'] != 'book_only_comparison' or not value['game']['required'], 'book-only policy')
    for name in ('window_ns', 'history_ns', 'exit_horizon_ns', 'decision_delay_ns', 'cooldown_after_close_ns', 'rearm_false_ns'):
        uint(value[name])
    require(0 < int(value['window_ns']) <= int(value['history_ns']) and int(value['exit_horizon_ns']) > 0 and int(value['rearm_false_ns']) > 0, 'history/horizon/rearm bounds')
    for name, maximum in (('history_changes', 100000), ('max_entries_per_event', 10000), ('max_open_per_target', 1), ('maximum_concurrent_positions', 256), ('max_books', 256), ('max_routes', 4096), ('models_tried', 100000), ('thresholds_tried', 100000)):
        require(type(value[name]) is int and 1 <= value[name] <= maximum, name)
    for name in ('minimum_leader_move', 'minimum_forecast_margin', 'minimum_forecast_return_bps', 'take_profit_fraction', 'stop_loss_fraction', 'transaction_budget', 'event_budget', 'maximum_outstanding_cost', 'holding_rate_per_ns'):
        number(value[name], nonnegative=True)
    require(number(value['minimum_leader_move']) > 0, 'positive leader threshold')
    require(number(value['stop_loss_fraction']) > 0, 'positive stop')
    for name in ('exit_on_next_segment_end', 'exit_on_match_end'):
        require(type(value[name]) is bool, 'exit flag')
    require(type(value['quantity_grid']) is list and 0 < len(value['quantity_grid']) <= 32, 'size grid bound')
    sizes = [number(x, nonnegative=True) for x in value['quantity_grid']]
    require(sizes == sorted(set(sizes)) and sizes[0] > 0, 'sorted positive size grid')
    require(value['capacity_mode'] in ('CUMULATIVE_DISPLAY_CAP', 'ISOLATED_NONADDITIVE'), 'capacity mode')
    require(value['trading_status_mode'] == 'TRADABLE_IF_USABLE_SCENARIO', 'trading status mode')
    valuation = obj(value['valuation'], 'mode weights')
    require(valuation['mode'] in ('SINGLE_NATIVE_ASSET', 'PARITY_SCENARIO', 'PINNED_VALUATION_STRESS_SCENARIO'), 'valuation mode')
    require(type(valuation['weights']) is dict and bool(valuation['weights']), 'valuation weights')
    for venue, weight in valuation['weights'].items():
        require(venue in ('kalshi', 'polymarket', 'limitless') and number(weight) > 0, 'valuation weight')
        require(valuation['mode'] == 'PINNED_VALUATION_STRESS_SCENARIO' or number(weight) == 1, 'parity/unit weight')
    cash = value['initial_cash']
    require(type(cash) is list and 0 < len(cash) <= 3, 'cash accounts')
    require([x['venue'] for x in cash] == sorted(set(x['venue'] for x in cash)), 'unique cash accounts')
    for item in cash:
        obj(item, 'venue asset amount'); obj(item['asset'], 'kind ledger token')
        require(item['venue'] in valuation['weights'], 'cash valuation')
        number(item['amount'], nonnegative=True)
    settlement = obj(value['settlement'], 'mode delay_ns immediate_availability evidence')
    require(settlement['mode'] in ('UNRESOLVED', 'NORMAL_RESOLUTION_SCENARIO', 'PINNED_SETTLEMENT'), 'settlement mode')
    uint(settlement['delay_ns'])
    require(type(settlement['immediate_availability']) is bool, 'settlement availability label')
    require(settlement['mode'] != 'NORMAL_RESOLUTION_SCENARIO' or int(settlement['delay_ns']) > 0 or settlement['immediate_availability'], 'immediate availability must be explicit')
    require(type(settlement['evidence']) is list and len(settlement['evidence']) <= 512, 'settlement evidence')
    for item in settlement['evidence']:
        obj(item, 'instrument orientation payout available_ns evidence_sha256'); number(item['payout'], nonnegative=True); uint(item['available_ns']); sha(item['evidence_sha256'])
    require(settlement['mode'] == 'PINNED_SETTLEMENT' or not settlement['evidence'], 'unexpected settlement evidence')
    require(type(value['rule_bindings']) is list and len(value['rule_bindings']) <= 256, 'rule bound')
    keys = []
    for rule in value['rule_bindings']:
        obj(rule, 'instrument orientation rule_sha256 alignment_id price_increment_atoms quantity_increment_atoms')
        sha(rule['rule_sha256']); require(type(rule['alignment_id']) is str and 0 < len(rule['alignment_id']) <= 128, 'rule alignment')
        for field in ('price_increment_atoms', 'quantity_increment_atoms'):
            require(int(uint(rule[field])) > 0, 'positive native increment')
        keys.append((rule['instrument'], rule['orientation']))
    require(keys == sorted(set(keys)), 'sorted unique rules')
    require(type(value['models']) is list and [x['role'] for x in value['models']] == list(ROLES), 'three ordered models')
    for model in value['models']:
        model_policy(model, value)
    fits = [m['training'] for m in value['models'] if m['provenance'] == 'PAST_EVENT_FIT']
    if fits:
        require(len(fits) == 3 and all(x == fits[0] for x in fits), 'nested models require same frozen past-event split')
    require(len(encoded(value)) <= 8 * 1024 * 1024, 'policy budget')
    return value


def model_policy(model, parent):
    obj(model, 'version role provenance assumption horizon_ns features relations cohorts fallback training')
    require(type(model['version']) is int and model['version'] == 1, 'model version')
    require(model['role'] in ROLES and model['provenance'] in ('ASSUMED_RESPONSE_MODEL', 'PAST_EVENT_FIT'), 'model mode')
    require(model['horizon_ns'] == parent['exit_horizon_ns'], 'model forecast horizon')
    require(model['features'] == ['delta_L', 'delta_T', 'released_milestones'], 'pinned V1 features')
    require(model['relations'] == ['IDENTITY', 'COMPLEMENT'], 'V1 supported relationship models')
    require(type(model['assumption']) is str and len(model['assumption']) <= 2048, 'model assumption')
    require(type(model['cohorts']) is list and 0 < len(model['cohorts']) <= 256, 'model cohorts bound')
    for cohort in model['cohorts']:
        obj(cohort, 'game phase score_quality prefix_quality home away elapsed_min_ns elapsed_max_ns beta_L beta_T intercept')
        require(cohort['game'] is None or type(cohort['game']) is str, 'cohort game')
        require(cohort['phase'] in ('pre_match', 'in_segment', 'between_segments', 'finished', 'unavailable'), 'released phase')
        require(cohort['score_quality'] in ('complete', 'unknown', 'book_only'), 'score quality')
        require(cohort['prefix_quality'] in ('complete', 'incomplete', 'book_only'), 'prefix quality')
        for key in ('home', 'away'):
            require(cohort[key] is None or type(cohort[key]) is int and cohort[key] >= 0, 'cohort score')
        uint(cohort['elapsed_min_ns'])
        if cohort['elapsed_max_ns'] is not None:
            uint(cohort['elapsed_max_ns']); require(int(cohort['elapsed_max_ns']) > int(cohort['elapsed_min_ns']), 'cohort elapsed interval')
        for key in ('beta_L', 'beta_T', 'intercept'):
            number(cohort[key])
        require(model['role'] == 'leader_milestones' or number(cohort['beta_L']) == 0, 'baseline cannot read leader')
        if model['role'] == 'target_only':
            require(cohort['score_quality'] == cohort['prefix_quality'] == 'book_only' and cohort['phase'] == 'unavailable', 'target-only must ignore milestones')
    require(model['fallback'] is None or type(model['fallback']) is int and 0 <= model['fallback'] < len(model['cohorts']), 'frozen fallback cohort')
    for i, left in enumerate(model['cohorts']):
        for right in model['cohorts'][i + 1:]:
            fields_overlap = all(left[k] is None or right[k] is None or left[k] == right[k] for k in ('game', 'home', 'away')) and all(left[k] == right[k] for k in ('phase', 'score_quality', 'prefix_quality'))
            left_end = int(left['elapsed_max_ns']) if left['elapsed_max_ns'] is not None else 2**64
            right_end = int(right['elapsed_max_ns']) if right['elapsed_max_ns'] is not None else 2**64
            require(not (fields_overlap and max(int(left['elapsed_min_ns']), int(right['elapsed_min_ns'])) < min(left_end, right_end)), 'overlapping model cohorts')
    if model['provenance'] == 'ASSUMED_RESPONSE_MODEL':
        require(model['training'] is None and bool(model['assumption']), 'declared model assumption')
    else:
        training = obj(model['training'], 'training_events validation_events cutoff_ns feature_definition target_definition hyperparameters_sha256 eligibility_sha256')
        uint(training['cutoff_ns']); sha(training['hyperparameters_sha256']); sha(training['eligibility_sha256'])
        require(training['feature_definition'] == 'committed_asof_midpoint_changes_and_released_milestones_v1' and training['target_definition'] == 'target_bid_ladder_displacement_at_horizon_v1', 'fit definitions')
        seen = set()
        for name in ('training_events', 'validation_events'):
            require(type(training[name]) is list and bool(training[name]) and len(training[name]) <= 10000, 'whole-event split')
            for item in training[name]:
                obj(item, 'event_id outcome_available_ns horizon_end_ns')
                require(type(item['event_id']) is str and item['event_id'] not in seen, 'distinct training/validation events')
                seen.add(item['event_id']); uint(item['outcome_available_ns']); uint(item['horizon_end_ns'])
                require(int(item['outcome_available_ns']) <= int(training['cutoff_ns']) and int(item['horizon_end_ns']) <= int(training['cutoff_ns']), 'purged fit cutoff')


def configuration(value, snapshot):
    value = obj(plain(value), 'version snapshot_directory snapshot_sha256 source_revision fees policy')
    require(type(value['version']) is int and value['version'] == 1, 'config version'); require(type(value['source_revision']) is str and re.fullmatch(r'(?:[0-9a-f]{40}|[0-9a-f]{64})', value['source_revision']) is not None, 'source Git/content revision'); sha(value['snapshot_sha256'])
    value['policy'] = policy(value['policy'])
    event = snapshot.get('outcomes', {}).get('document', {}).get('event_id')
    for model in value['policy']['models']:
        if model['training'] is not None:
            require(int(model['training']['cutoff_ns']) < int(snapshot['config']['start_ns']), 'fit available before first decision')
            require(event not in {x['event_id'] for k in ('training_events', 'validation_events') for x in model['training'][k]}, 'evaluated event held out')
    return value


def semantic(config):
    fees = {k: v for k, v in config['fees'].items() if k != 'catalog_directory'}
    return {'version': 1, 'strategy': STRATEGY, 'snapshot_sha256': config['snapshot_sha256'], 'source_revision': config['source_revision'], 'fees': fees, 'policy': experiment_policy(config['policy'])}


def identity(config):
    return digest(semantic(config))

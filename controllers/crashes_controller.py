from flask import Blueprint, request, jsonify

from repository.crashes_repository import find_total_accidents_in_area, find_total_accidents, \
    find_accidents_grouped_by_cause, extract_area_statistics
from repository.csv_repository import init_accidents

crashes_blueprint = Blueprint("crashes", __name__)

@crashes_blueprint.route('/area', methods=['GET'])
def total_accidents_in_area():
    beat = request.args.get('beat')
    if not beat:
        return jsonify({'error': 'beat parameter is required'}), 400
    try:
        total_accidents = find_total_accidents_in_area(str(beat))
        return jsonify({'total_accidents': total_accidents}), 200
    except Exception as e:
        return jsonify({'error': repr(e)}), 500


@crashes_blueprint.route('/initialize-db', methods=['GET'])
def init_database():
    try:
        init_accidents()
        return jsonify({'success': True}), 200
    except Exception as e:
        return jsonify({'error': repr(e)}), 500


@crashes_blueprint.route('/total', methods=['GET'])
def total_accidents_route():
    # Get parameters from the query string
    period = request.args.get('period')
    date = request.args.get('date')
    area = request.args.get('area')

    # Validate parameters
    if not period or not date or not area:
        return jsonify({"error": "Missing required parameters: area, period, date."}), 400

    try:
        total = find_total_accidents(period, date, area)
        return jsonify({"total_accidents": total}), 200
    except ValueError as e:
        return jsonify({"error": repr(e)}), 400
    except Exception as e:
        return jsonify({"error": repr(e)}), 500

@crashes_blueprint.route('/area/causes', methods=['GET'])
def accidents_causes_in_area():
    area = request.args.get('beat')
    if not area:
        return jsonify({'error': 'area parameter is required'}), 400
    try:
        result = find_accidents_grouped_by_cause(area)
        return jsonify(result), 200
    except ValueError as ve:
        return jsonify({'error': repr(ve)}), 400
    except Exception as e:
        return jsonify({"error": repr(e)}), 500

@crashes_blueprint.route('/area/statistics', methods=['GET'])
def accidents_statistics_in_area():
    area = request.args.get('beat')
    if not area:
        return jsonify({'error': 'area parameter is required'}), 400
    try:
        result = extract_area_statistics(area)
        return jsonify(result), 200
    except ValueError as ve:
        return jsonify({'error': repr(ve)}), 400
    except Exception as e:
        return jsonify({"error": repr(e)}), 500
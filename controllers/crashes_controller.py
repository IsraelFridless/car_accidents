from flask import Blueprint, request, jsonify

from repository.crashes_repository import find_total_accidents_in_area, get_total_accidents
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


@crashes_blueprint.route('/init_database', methods=['GET'])
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
        total = get_total_accidents(period, date, area)
        return jsonify({"total_accidents": total}), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": "An error occurred while processing your request."}), 500
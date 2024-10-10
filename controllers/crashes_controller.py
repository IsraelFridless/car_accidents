from flask import Blueprint, request, jsonify

from repository.crashes_repository import find_total_accidents_in_area

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


import threading

import app

events_listener_thread = threading.Thread(
    daemon=True,
    name="WS Event Listener",
    target=app.events.events_listener,
)
events_listener_thread.start()

server = app.create_app()

if __name__ == '__main__':
    server.run(debug=app.config['DEBUG'], use_reloader=False, host="0.0.0.0", port=6000)

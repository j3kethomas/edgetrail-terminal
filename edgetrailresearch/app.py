from dash import Dash,html

app = Dash()

app.layout = [
    html.Div(children="Hello")
]
if __name__ == "__main__":
    app.run(debug=True)
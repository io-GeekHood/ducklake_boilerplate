import panel as pn
import numpy as np
import matplotlib.pyplot as plt
import os
from lake.connector import load
blacklist = ['__init__.py','core.py','kafka.py','__pycache__']
available_modules = [filename.replace('.py','') for filename in os.listdir('lake/connector/') if filename not in blacklist]
# Ensure the Panel extension is loaded
pn.extension()

# Function to generate a plot
def generate_plot(name):
    """Generate a random plot and display with the given name."""
    plt.figure(figsize=(6, 4))
    data = np.random.randn(100).cumsum()
    plt.plot(data)
    plt.title(name)
    plt.xlabel("Index")
    plt.ylabel("Value")
    plt.grid()
    
    # Return the current figure object instead of the module
    return plt.gcf()  # Get current figure

# Function to render a dashboard
def render_dashboard(module_name):
    """Render the dashboard with the given name."""
    print(f"LOADING MODULE {module_name}")
    instance = load(module_name,'resources/config.tmp.yml')
    plot = instance.deploy()
    dashboard = pn.Column(pn.pane.Matplotlib(plot, tight=True), width=600)
    return dashboard

# Sidebar with navigation
sidebar = pn.widgets.Select(name='Select Dashboard', options=[available_modules])

# Creating initial dashboards
dashboards = {name: render_dashboard(name) for name in sidebar.options}

# Function to update the displayed dashboard
def view_dashboard(event):
    selected_dashboard = event.new
    dashboard_panel[:] = [dashboards[selected_dashboard]]

sidebar.param.watch(view_dashboard, 'value')

# Initial dashboard
dashboard_panel = pn.Column(dashboards[sidebar.value])
layout = pn.Row(sidebar, dashboard_panel)

# Display the layout
main_layout = pn.Column(layout)
main_layout.servable(title='Multi-Dashboard Application')

# Run the app
if __name__ == '__main__':
    pn.serve(main_layout)
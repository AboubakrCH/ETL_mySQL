<script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
  <script type="text/javascript">
    google.charts.load("current", {packages:['corechart']});
    google.charts.setOnLoadCallback(drawChart);
    
    function drawChart() {
      var data = google.visualization.arrayToDataTable([
        ["Syntaxique Type", "Occurence", { role: "style" } ],
        ["STRING", 8.94, "#FF7F50"],
        ["NUMBER", 10.49, "#8B008B"],
        ["DATE", 19.30, "#7FFFD4"],
        ["BOOLEAN", 21.45, "color: #FFD700"],
        ["NULL", 21.45, "color: #00CED1"]
      ]);

      var view = new google.visualization.DataView(data);
      view.setColumns([0, 1,
                       { calc: "stringify",
                         sourceColumn: 1,
                         type: "string",
                         role: "annotation" },
                       2]);
      
      var options = {
        title: "The DOMINANT Syntactic type ",
        width: 600,
        height: 400,
        bar: {groupWidth: "95%"},
        legend: { position: "none" },
      }; 
      var chart = new google.visualization.ColumnChart(document.getElementById("columnchart_values"));
      chart.draw(view, options);
  }
  </script>
<div id="columnchart_values" style="width: 900px; height: 300px;"></div>













    google.charts.setOnLoadCallback(drawChartCircle);

  function drawChartCircle() {

          var data = google.visualization.arrayToDataTable([
            ['Task', 'Hours per Day'],
            ['Normal',     11],
            ['Annormal',      2]
          ]);

          var options = {
            title: 'The sementique Anomalies'
          };

          var chart = new google.visualization.PieChart(document.getElementById('piechart'.concat(<?php echo $i ;?>));

          chart.draw(data, options);
        }

  </script>
  

<?php

  echo '<div style="width: 1250px; height: 300px;border: 1px solid #333;box-shadow: 8px 8px 5px #444;padding: 8px 12px;">';

  echo '<div id="columnchart'.$i.'"></div>' ;
  echo '<div id="piechart'.$i.'"></div>' ;

  echo '</div>';
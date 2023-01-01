import React, { useEffect } from "react";
import * as d3 from "d3";
import './chart_prediction.css';


const quarter_to_month = {
    // end month of each season
    1: '8',
    2: '11',
    3: '2',
    4: '5'
};


/**
 * convert quarter date (year quarter) to return date (year, month)
 */
function return_date(Year, Quarter){
    return {"Year": Year + (Quarter >= 3), "Month": quarter_to_month[Quarter]}
}

function SeasonReturnChart(props) {
    let data = props.data; // json
    
    d3.selectAll('svg').remove(); // clean old graphs

    const createGraph = () => {
        
        if (data.length > 1){        
            let parseTime = d3.timeParse("%Y-%m");

            for (let i = 0; i < data.length; i++) {
                let {Year, Month} = return_date(data[i].Year, data[i].Quarter)
                
                data[i].date = parseTime(`${Year}-${Month}`);
            }
            var startTime = data[0].date;

            // additional timestamp at the end
            let {Year, Month} = return_date(data[data.length-1].Year + (data[data.length-1].Quarter == 4), (data[data.length-1].Quarter %4) + 1)
            var endTime = parseTime(`${Year}-${Month}`);
            console.log("graph input data: ", data)
                
            // set the dimensions and margins of the graph
            var margin = { top: 20, right: 20, bottom: 50, left: 70 },
                width = 600 - margin.left - margin.right,
                height = 400 - margin.top - margin.bottom;

            var svg = d3.select("#season_return_chart").append("svg")
            // .attr("width", width + margin.left + margin.right)
            // .attr("height", height + margin.top + margin.bottom)
            .attr("preserveAspectRatio", "xMinYMin meet")
            .attr("viewBox", "0 0 600 500")
            .append("g")
            .attr("transform", `translate(${margin.left}, ${margin.top})`);

            // create X axis and Y axis
            var x = d3.scaleTime().range([0, width]);
            var y = d3.scaleLinear().range([height, 0]);

            let y_min = d3.min(data, (d) => { return d.Stock_Return; });
            let y_max = d3.max(data, (d) => { return d.Stock_Return; });

            x.domain([startTime, endTime]);
            y.domain([y_min - (y_max-y_min)/10 ,y_max + (y_max-y_min)/10 ]);

            // x-axis
            svg.append("g")
                .attr("transform", `translate(0, ${height})`)
                .call(d3.axisBottom(x).ticks(5).tickFormat(d3.timeFormat('%Y/%m')));  // adjust x-axis ticks

            // y-axis
            svg.append("g")
                .call(d3.axisLeft(y));


            // add the line
            var valueLine = d3.line()
                .x((d) => { return x(d.date); })
                .y((d) => { return y(d.Stock_Return); });

            svg.append("path")
                .data([data])
                .attr("class", "line")
                .attr("fill", "none")
                .attr("stroke", "orange")
                .attr("stroke-width", 2.0)
                .attr("d", valueLine);

            // render static points
            for (let i = 0; i < data.length; i++) { 
                var color = "orange";

                var point = svg
                .append('g')
                .append('circle')
                  .style("fill", color)
                  .attr("stroke", "orange")
                  .attr('r', 5)
                  .style("opacity", 1)
                  .attr("cx", x(data[i].date))
                  .attr("cy", y(data[i].Stock_Return));
              }
        }

    };

    useEffect(() => {
        createGraph();
    }, [props]);

    return ( 
        <div id="season_return_chart">
            <h3>Parameters: </h3>
            <li>Model: { props.data[0].Model }</li>
            <li>Strategy: { props.data[0].Strategy } </li>
        </div>
    );
}

function AccumulatedReturnChart(props) {
    let data = props.data; // json
    var {Year: startYear, Month: startMonth} = return_date(data[0].Year, data[0].Quarter);
    var {Year: endYear, Month: endMonth} = return_date(data[data.length - 1].Year, data[data.length - 1].Quarter);

    d3.selectAll('svg').remove(); // clean old graphs

    const createGraph = () => {
        
        if (data.length > 1){        
            let parseTime = d3.timeParse("%Y-%m");

            for (let i = 0; i < data.length; i++) {
                let {Year, Month} = return_date(data[i].Year, data[i].Quarter)
                
                data[i].date = parseTime(`${Year}-${Month}`);
            }
            var startTime = data[0].date;

            // additional timestamp at the end
            let {Year, Month} = return_date(data[data.length-1].Year + (data[data.length-1].Quarter == 4), (data[data.length-1].Quarter %4) + 1)
            var endTime = parseTime(`${Year}-${Month}`);
            console.log("graph input data: ", data)
                
            // set the dimensions and margins of the graph
            var margin = { top: 20, right: 20, bottom: 50, left: 70 },
                width = 600 - margin.left - margin.right,
                height = 400 - margin.top - margin.bottom;

            var svg = d3.select("#acc_return_chart").append("svg")
            // .attr("width", width + margin.left + margin.right)
            // .attr("height", height + margin.top + margin.bottom)
            .attr("preserveAspectRatio", "xMinYMin meet")
            .attr("viewBox", "0 0 600 500")
            .append("g")
            .attr("transform", `translate(${margin.left}, ${margin.top})`);

            // create X axis and Y axis
            var x = d3.scaleTime().range([0, width]);
            var y = d3.scaleLinear().range([height, 0]);

            let y_min = d3.min(data, (d) => { return d.Stock_Return; });
            let y_max = d3.max(data, (d) => { return d.Stock_Return; });

            x.domain([startTime, endTime]);
            y.domain([y_min - (y_max-y_min)/10 ,y_max + (y_max-y_min)/10 ]);

            // x-axis
            svg.append("g")
                .attr("transform", `translate(0, ${height})`)
                .call(d3.axisBottom(x).ticks(5).tickFormat(d3.timeFormat('%Y/%m')));  // adjust x-axis ticks

            // y-axis
            svg.append("g")
                .call(d3.axisLeft(y));


            // add the line
            var valueLine = d3.line()
                .x((d) => { return x(d.date); })
                .y((d) => { return y(d.Stock_Return); });

            svg.append("path")
                .data([data])
                .attr("class", "line")
                .attr("fill", "none")
                .attr("stroke", "orange")
                .attr("stroke-width", 2.0)
                .attr("d", valueLine);

            // render static points
            for (let i = 0; i < data.length; i++) { 
                var color = "orange";

                if (i == 0)
                    color = "rgb(0, 255, 0)"; // light green
                else if (i == data.length - 1)
                    color = "rgb(255, 0, 0)";   // red

                var point = svg
                .append('g')
                .append('circle')
                  .style("fill", color)
                  .attr("stroke", "orange")
                  .attr('r', 5)
                  .style("opacity", 1)
                  .attr("cx", x(data[i].date))
                  .attr("cy", y(data[i].Stock_Return));

              }
            
              // add highlight text
                svg
                .append('g')
                .append('text')
                  .style("fill", "rgb(0, 255, 0)")
                  .style("opacity", 1)
                  .attr("x", x(data[0].date)+10)
                  .attr("y", y(data[0].Stock_Return)-20)
                  .attr("text-anchor", "left")
                  .attr("alignment-baseline", "middle")
                  .text("Start Investment");
                svg
                .append('g')
                .append('text')
                  .style("fill", "rgb(255, 0, 0)")
                  .style("opacity", 1)
                  .attr("x", x(data[data.length - 1].date)-70)
                  .attr("y", y(data[data.length - 1].Stock_Return)-20)
                  .attr("text-anchor", "left")
                  .attr("alignment-baseline", "middle")
                  .text("Expected Return (now)");
        }

    };

    useEffect(() => {
        createGraph();
    }, [props]);

    return ( 
        <div id="acc_return_chart">
            <h3>Parameters: </h3>
            <li>Model: { props.data[0].Model }</li>
            <li>Strategy: { props.data[0].Strategy } </li>
            <li>Investment start date: { startYear }/{startMonth}/20</li>
            <li>Expected Return date: { endYear }/{endMonth}/20</li>
        </div>
    );
}

export {SeasonReturnChart, AccumulatedReturnChart};
import './App_prediction.css';

import React, { useState, useEffect } from "react";


import {SeasonReturnChart, AccumulatedReturnChart} from './chart_prediction';


import Container from 'react-bootstrap/Container';
import Row from 'react-bootstrap/Row';
import Col from 'react-bootstrap/Col';
import 'bootstrap/dist/css/bootstrap.min.css';

import axios from "axios";


// Environment variables: https://create-react-app.dev/docs/adding-custom-environment-variables/#expanding-environment-variables-in-env
const FrontendBaseURL = process.env.REACT_APP_FrontendBaseURL;
const BackendBaseURL = process.env.REACT_APP_BackendBaseURL;


function App_prediction(props) {
    var [Year, setYear] = useState(2020);
    var [Quarter, setQuarter] = useState(1);
    var [Model, setModel] = useState("FNN");
    var [Strategy, setStrategy] = useState("equal weight");

    const [Rank, setRank] = useState([]);
    const [SeasonReturn, setSeasonReturn] = useState([{}]);
    const [AccReturn, setAccReturn] = useState([{}]);

  // Fetch data from backend
    const fetchRank = async (Model) => {
        try {
            const response = await axios.get(
            `${BackendBaseURL}/stock_prediction/rank?Model=${Model}`
            );
            
            let newData = response.data;
            setRank(newData);
            console.log(newData);
        }catch(err){
            setRank([]);
            console.log(err);
        }
        
    };

    const fetchSeasonReturn = async (Model, Strategy) => {
        try {
            const response = await axios.get(
            `${BackendBaseURL}/stock_prediction/individual_return?Model=${Model}&Strategy=${Strategy}`
            );

            let newData = response.data;
            setSeasonReturn(newData);
            console.log(newData);
        }catch(err){
            setSeasonReturn([{}]);
            console.log(err);
        }
    };

    const fetchAccReturn = async (Year, Quarter, Model, Strategy) => {
        try {
            const response = await axios.get(
            `${BackendBaseURL}/stock_prediction/accumulated_return?Year=${Year}&Quarter=${Quarter}&Model=${Model}&Strategy=${Strategy}`
            );

            let newData = response.data;
            setAccReturn(newData);
            console.log(newData);
        }catch(err){
            setAccReturn([{}]);
            console.log(err);
        }
    };


    useEffect(() => {
        fetchRank(Model);
        fetchSeasonReturn(Model, Strategy);
        fetchAccReturn(Year, Quarter, Model, Strategy);
        console.log("useEffect:", Year, Quarter, Model, Strategy)
    }, [Year, Quarter, Model, Strategy]);

    // fetchRank(Model);
    // fetchSeasonReturn(Model, Strategy);
    // fetchAccReturn(Year, Quarter, Model, Strategy);
    // useEffect(() => {
    //     fetchRank(Model);
    //     console.log("Rank:", Year, Quarter, Model, Strategy)
    // }, [Model]);

    // useEffect(() => {
    //     fetchSeasonReturn(Model, Strategy);
    //     console.log("SeasonReturn:", Year, Quarter, Model, Strategy)
    // }, [Model, Strategy]);

    // useEffect(() => {
    //     fetchAccReturn(Year, Quarter, Model, Strategy);
    //     console.log("AccReturn:", Year, Quarter, Model, Strategy)
    // }, [Year, Quarter, Model, Strategy]);
    

    function configMethodForm(event){
        // console.log(event);
        event.preventDefault();

        let Model = document.getElementById("Model").value;
        let Strategy = document.getElementById("Strategy").value;
        setModel(Model);
        setStrategy(Strategy);

    }

    function configDateForm(event){
        console.log(event);
        event.preventDefault();
  
        let Year = document.getElementById("Year").value;
        let Quarter = document.getElementById("Quarter").value;
        setYear(Year);
        setQuarter(Quarter);
    }

    if(Rank.length == 0){
        // initial state
        return(<></>);
    } else {
        return (
        <Container fluid style={{'width': '90%', 'margin': '30px'}}>
            <Row>
                <h1> Stock Prediction </h1>
                <form id="methodForm" onSubmit={configMethodForm}>&nbsp;&nbsp;&nbsp;
                    <label for="Model">Model:</label>&nbsp;&nbsp;
                    <select name="Model" id="Model">
                        <option value="FNN">Feedforward Neural Network</option>
                        <option value="RF">Random Forest</option>
                    </select>
                    &nbsp;&nbsp;&nbsp;
                    <label for="Strategy">Strategy:</label>&nbsp;&nbsp;
                    <select name="Strategy" id="Strategy">
                        <option value="equal weight">equal weight</option>
                    </select>
                    &nbsp;&nbsp;
                    <input type="submit" value="Simulate"/> 
                    </form> 
            </Row>
            <Row>
                <Col id="season_return">
                    <div>
                        <h3> Seasonal Return </h3>
                        <SeasonReturnChart data={SeasonReturn}/>
                    </div>
                </Col>
                <Col id="acc_return">
                    <div>
                        <h3> Accumulated Return </h3>
                        <form id="dateForm" onSubmit={configDateForm}>
                        <label for="Year">Investment Year:</label>
                        &nbsp;&nbsp;
                        <input type="text" id="Year" name="Year" placeholder="2019" size="6" required/>
                        &nbsp;&nbsp;&nbsp;
                        <label for="Quarter">Investment Quarter&#40;1~4&#41;:</label>
                        &nbsp;&nbsp;
                        <input type="text" id="Quarter" name="Quarter" placeholder="1" size="2" required/>
                        &nbsp;&nbsp;
                        <input type="submit" value="Simulate"/> 
                        </form> 
                        <AccumulatedReturnChart data={AccReturn}/>
                    </div>
                </Col>
            </Row>
            <Row id="rank">
                <div>
                    <h3> Stock Rank Prediction </h3>
                    <li> Model: {Rank.Model} </li>
                    <br></br>
                    <ol>
                        {Rank.co_id_rank.map((co, index) => {
                            if (index < 25)
                                return(
                                    <li style={{color: "rgb(0,255,0)"}}> {co} </li>
                                );
                            else
                                return(
                                    <li> {co} </li>
                                );
                        })}
                    </ol>
                </div>
            </Row>
        </Container>
        );
    }
}

export default App_prediction;
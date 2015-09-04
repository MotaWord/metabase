"use strict";

import React, { Component, PropTypes } from "react";

import Greeting from "metabase/lib/greeting";
import Icon from "metabase/components/Icon.react";

import HeaderTabs from "./HeaderTabs.react";
import Activity from "./Activity.react";
import Cards from "./Cards.react";
import RecentViews from "./RecentViews.react";
import CardFilters from "./CardFilters.react";


export default class Homepage extends Component {

    constructor(props) {
        super(props);

        this.state = {
            greeting: Greeting.simpleGreeting()
        };

        this.styles = {
            main: {
                width: "auto",
                marginRight: "346px",
                borderWidth: "2px"
            },
            mainWrapper: {
                width: "100%",
                margin: "0 auto",
                paddingLeft: "12em",
                paddingRight: "3em"
            },
            sidebar: {
                width: "346px",
                backgroundColor: "#F9FBFC"
            },
            headerGreeting: {
                fontSize: "x-large"
            }
        };
    }

    render() {
        console.log('props=', this.props);
        const { selectedTab, user } = this.props;

        return (
            <div>
                <div className="bg-brand text-white">
                    <div style={this.styles.main}>
                        <div style={this.styles.mainWrapper}>
                            <header style={this.styles.headerGreeting} className="pb4">
                                <span className="float-left"><Icon className="mr3" name={'star'} height={36} width={36}></Icon></span>
                                <span>{(user) ? this.state.greeting + ' ' + user.first_name : this.state.greeting}</span>
                            </header>
                            <div className="">
                                <span className="float-left text-brand"><Icon className="mr3" name={'star'} height={36} width={36}></Icon></span>
                                <HeaderTabs {...this.props} />
                            </div>
                        </div>
                    </div>
                </div>
                <div className="relative">
                    <div style={this.styles.main} className="border-right">
                        <div style={this.styles.mainWrapper}>
                            { selectedTab === 'activity' ?
                                <Activity {...this.props} />
                            :
                                <Cards {...this.props} />
                            }
                        </div>
                    </div>
                    <div style={this.styles.sidebar} className="absolute top right">
                        { selectedTab === 'activity' ?
                            <RecentViews {...this.props} />
                        :
                            <CardFilters {...this.props} />
                        }
                    </div>
                </div>
            </div>
        );
    }
}

Homepage.propTypes = {
    dispatch: PropTypes.func.isRequired,
    onChangeLocation: PropTypes.func.isRequired
};

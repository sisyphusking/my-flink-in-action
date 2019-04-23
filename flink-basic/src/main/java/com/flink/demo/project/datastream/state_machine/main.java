package com.flink.demo.project.datastream.state_machine;

import com.flink.demo.project.datastream.state_machine.event.Alert;
import com.flink.demo.project.datastream.state_machine.event.Event;
import com.flink.demo.project.datastream.state_machine.event.State;
import com.flink.demo.project.datastream.state_machine.generator.EventsGeneratorSource;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

public class main {

    public static void main(String[] args) throws  Exception {

        final SourceFunction<Event> source;


        //有迭代器生成数据
        source = new EventsGeneratorSource(0.4, 1000);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);

        DataStream<Event> events = env.addSource(source);

        DataStream<Alert> alerts = events
                // partition on the address to make sure equal addresses
                // end up in the same state machine flatMap function
                .keyBy(Event::sourceAddress)
                // the function that evaluates the state machine over the sequence of events
                .flatMap(new StateMachineMapper());

        alerts.print().setParallelism(1);
        env.execute("State machine job");

    }


    static class StateMachineMapper extends RichFlatMapFunction<Event, Alert> {

        /**
         * The state for the current key.
         */
        private ValueState<State> currentState;

        @Override
        public void open(Configuration conf) {
            // get access to the state object
            currentState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("state", State.class));
        }

        @Override
        public void flatMap(Event evt, Collector<Alert> out) throws Exception {
            // get the current state for the key (source address)
            // if no state exists, yet, the state must be the state machine's initial state
            State state = currentState.value();
            if (state == null) {
                state = State.Initial;
            }

            // ask the state machine what state we should go to based on the given event
            State nextState = state.transition(evt.type());

            if (nextState == State.InvalidTransition) {
                // the current event resulted in an invalid transition
                // raise an alert!
                out.collect(new Alert(evt.sourceAddress(), state, evt.type()));
            } else if (nextState.isTerminal()) {
                // we reached a terminal state, clean up the current state
                currentState.clear();
            } else {
                // remember the new state
                currentState.update(nextState);
            }
        }

    }
}
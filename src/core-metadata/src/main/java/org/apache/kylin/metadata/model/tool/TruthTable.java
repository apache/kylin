/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.metadata.model.tool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;

public class TruthTable {

    private static Logger logger = LoggerFactory.getLogger(TruthTable.class);

    private List<Expr> allOperands;
    private Expr expr;

    private TruthTable(List<Expr> allOperands, Expr expr) {
        this.allOperands = allOperands;
        this.expr = expr;
    }

    public static boolean equals(TruthTable tbl1, TruthTable tbl2) {
        logger.debug("Comparing table1: {} with table2 {}", tbl1, tbl2);
        if (tbl1.allOperands.size() != tbl2.allOperands.size()) {
            return false;
        }
        for (Expr operand : tbl1.allOperands) {
            if (!tbl2.allOperands.contains(operand)) {
                return false;
            }
        }

        InputGenerator tbl1InputGenerator = tbl1.createInputGenerator();
        while (tbl1InputGenerator.hasNext()) {
            Input input = tbl1InputGenerator.next();
            if (tbl1.eval(input) != tbl2.eval(input.mapOperands(tbl2.allOperands))) {
                return false;
            }
        }
        return true;
    }

    public boolean eval(Input input) {
        return doEval(expr, input);
    }

    private boolean doEval(Expr expr, Input input) {
        switch (expr.operator) {
        case IDENTITY:
            return input.getValue(expr);
        case NOT:
            return !doEval(expr.exprs[0], input);
        case AND:
            for (Expr innerExpr : expr.exprs) {
                if (!doEval(innerExpr, input)) {
                    return false;
                }
            }
            return true;
        case OR:
            for (Expr innerExpr : expr.exprs) {
                if (doEval(innerExpr, input)) {
                    return true;
                }
            }
            return false;
        default:
            throw new IllegalStateException("Invalid Operator" + expr.operator);
        }
    }

    private static class Input {
        private Map<Expr, Boolean> inputValues;

        Input(Map<Expr, Boolean> inputValues) {
            this.inputValues = inputValues;
        }

        boolean getValue(Expr expr) {
            return inputValues.get(expr);
        }

        /**
         * map input with operands in different orderings
         * @param operands
         * @return
         */
        Input mapOperands(List<Expr> operands) {
            Preconditions.checkArgument(operands.size() == inputValues.size());
            Map<Expr, Boolean> mappedInput = new HashMap<>();
            for (Expr expr : operands) {
                Preconditions.checkArgument(inputValues.get(expr) != null, "Invalid table expr for operands mapping");
                mappedInput.put(expr, inputValues.get(expr));
            }
            Preconditions.checkArgument(mappedInput.size() == inputValues.size());
            return new Input(mappedInput);
        }
    }

    public InputGenerator createInputGenerator() {
        return new InputGenerator();
    }

    public class InputGenerator implements Iterator<Input> {

        int inputSize = allOperands.size();
        int inputMax = (int) Math.pow(2, inputSize);
        int inputBits = 0;

        @Override
        public boolean hasNext() {
            return inputBits < inputMax;
        }

        @Override
        public Input next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Map<Expr, Boolean> input = new HashMap<>();
            if (logger.isTraceEnabled()) {
                logger.trace("Generating next input {}, max inputValues bits {}", Integer.toBinaryString(inputBits),
                        Integer.toBinaryString(inputMax - 1));
            }
            for (int i = 0; i < inputSize; i++) {
                boolean value = (inputBits & (1 << i)) != 0;
                input.put(allOperands.get(i), value);
            }
            inputBits++;
            return new Input(input);
        }
    }

    enum Operator {
        AND, OR, NOT, IDENTITY
    }

    private static class Expr<T> {
        Operator operator;
        Expr[] exprs = new Expr[0];
        T operandRef;
        Comparator<T> operandComparator;

        Expr(Operator operator, Expr[] exprs, Comparator<T> operandComparator) {
            this.operator = operator;
            this.exprs = exprs;
            this.operandComparator = operandComparator;
        }

        Expr(T operandRef, Comparator<T> operandComparator) {
            this.operator = Operator.IDENTITY;
            this.operandRef = operandRef;
            this.operandComparator = operandComparator;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof Expr)) {
                return false;
            }

            Expr that = (Expr) obj;
            if (!(operator == that.operator && exprs.length == that.exprs.length
                    && operandComparator.compare(operandRef, (T) that.operandRef) == 0)) {
                return false;
            }
            for (int i = 0; i < exprs.length; i++) {
                if (!Objects.equals(exprs[i], that.exprs[i])) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + operator.hashCode();
            result = prime * result + operandRef.hashCode();
            for (Expr expr : exprs) {
                result = prime * result + expr.hashCode();
            }
            return result;
        }

        @Override
        public String toString() {
            return "Expr{" + "operator=" + operator + ", exprs=" + Arrays.toString(exprs) + ", operandRef=" + operandRef
                    + '}';
        }
    }

    @Override
    public String toString() {
        return "TruthTable{" + "allOperands=" + allOperands + ", expr=" + expr + '}';
    }

    public static class TruthTableBuilder<T> extends ExprBuilder<T> {

        public TruthTableBuilder(Comparator<T> operandComparator) {
            super(operandComparator);
        }

        public TruthTable build() {
            Expr expr = buildExpr();
            return new TruthTable(new ArrayList<>(allOperandSet), expr);
        }
    }

    public static class ExprBuilder<T> {
        Set<Expr> allOperandSet = new HashSet<>();
        Deque<Operator> operatorStack = new ArrayDeque<>();
        Deque<List<Expr>> exprsStack = new ArrayDeque<>();
        Comparator<T> operandComparator;

        public ExprBuilder(Comparator<T> operandComparator) {
            this.operandComparator = operandComparator;
            exprsStack.push(new LinkedList<>());
        }

        public ExprBuilder compositeStart(Operator operator) {
            operatorStack.push(operator);
            exprsStack.push(new LinkedList<>());
            return this;
        }

        public ExprBuilder compositeEnd() {
            Expr<T> composited = new Expr<>(operatorStack.pop(), exprsStack.pop().toArray(new Expr[0]),
                    operandComparator);
            exprsStack.peek().add(composited);
            return this;
        }

        public ExprBuilder addOperand(T operandRef) {
            Expr<T> expr = new Expr<>(operandRef, operandComparator);
            allOperandSet.add(expr);
            exprsStack.peek().add(expr);
            return this;
        }

        protected Expr buildExpr() {
            return exprsStack.peek().get(0);
        }
    }
}

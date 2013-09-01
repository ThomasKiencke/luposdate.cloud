package lupos.cloud.optimizations.logical.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.omg.PortableInterceptor.SUCCESSFUL;

import lupos.cloud.operator.IndexScanContainer;
import lupos.cloud.operator.MultiIndexScanContainer;
import lupos.cloud.pig.operator.PigFilterOperator;
import lupos.datastructures.items.Variable;
import lupos.engine.operators.BasicOperator;
import lupos.engine.operators.OperatorIDTuple;
import lupos.engine.operators.multiinput.MultiInputOperator;
import lupos.engine.operators.singleinput.Projection;
import lupos.engine.operators.singleinput.Result;
import lupos.engine.operators.singleinput.filter.Filter;
import lupos.engine.operators.singleinput.modifiers.distinct.Distinct;
import lupos.engine.operators.singleinput.sort.Sort;

public class OperatorGraphHelper {

	// Operationen werden zurück gegeben und im Graphen GELÖSCHT!
	public static ArrayList<BasicOperator> getAndDeleteOperationUntilNextMultiInputOperator(
			List<OperatorIDTuple> succeedingOperators) {
		ArrayList<BasicOperator> result = new ArrayList<BasicOperator>();
		ArrayList<OperatorIDTuple> opPool = new ArrayList<OperatorIDTuple>(
				succeedingOperators);
		while (opPool.size() > 0) {
			OperatorIDTuple opID = opPool.get(0);
			BasicOperator op = opID.getOperator();
			if (op instanceof MultiInputOperator || op instanceof Result) {
				opPool.remove(opID);
				break;
			} else {
				// Nachfolger merken
				opPool.addAll(op.getSucceedingOperators());

				// Operation löschen und Kanten "säubern"
				op.removeFromOperatorGraph();
				op.setSucceedingOperators(new LinkedList<OperatorIDTuple>());
				op.setPrecedingOperators(new LinkedList<BasicOperator>());

				result.add(op);

				opPool.remove(opID);

			}
		}
		return result;
	}

	public static boolean isOperationSupported(BasicOperator op) {
		boolean result = true;
		if (op instanceof Filter) {
			return PigFilterOperator.checkIfFilterIsSupported(((Filter) op)
					.getNodePointer().getChildren()[0]);
		}
		// System.out
		// .println("Der Filter \""
		// + filter.toString().replace("\n", "")
		// +
		// "\" wird nicht in der Cloud ausgeführt, da nicht alle Variablen im IndexScan-Operator vorhanden sind.");
		return result;
	}

	/*
	 * Bestimmte Operationen werden nicht verschoben sondern bleiben im
	 * äußersten Container. Anmerkung: Äußere Projektionen (=globale
	 * Projektionen) sind trotzdem in ALLEN unteren Containern "aktiv"
	 */
	// public static boolean moveOperation(BasicOperator op) {
	// boolean result = true;
	// if (op instanceof Projection || op instanceof Distinct) {
	// result = false;
	// }
	// return result;
	// }

	public static void replaceOperation(BasicOperator oldOp, BasicOperator newOp) {
		// Alte Vorgänger/Nachfolger merken
		final Collection<BasicOperator> preds = oldOp.getPrecedingOperators();
		final List<OperatorIDTuple> succs = oldOp.getSucceedingOperators();

		// IndexScan durch Container austauschen
		for (final BasicOperator pred : preds) {
			pred.getOperatorIDTuple(oldOp).setOperator(newOp);
			newOp.addPrecedingOperator(pred);
		}

		newOp.setSucceedingOperators(succs);

		for (final OperatorIDTuple succ : succs) {
			succ.getOperator().removePrecedingOperator(oldOp);
			succ.getOperator().addPrecedingOperator(oldOp);
		}
	}

	public static BasicOperator getNextMultiInputOperation(BasicOperator op) {
		if (op instanceof MultiInputOperator) {
			return op;
		} else {
			for (OperatorIDTuple succ : op.getSucceedingOperators()) {
				return getNextMultiInputOperation(succ.getOperator());
			}
		}
		return null;
	}

	public static Collection<Variable> getUnionVariablesFromMultipleOperations(
			Set<BasicOperator> list) {
		HashSet<Variable> result = new HashSet<Variable>();

		for (BasicOperator op : list) {
			for (Variable var : op.getUnionVariables()) {
				result.add(var);
			}
		}
		return new ArrayList<Variable>(result);
	}

	public static Collection<Variable> getIntersectionVariablesFromMultipleOperations(
			Set<BasicOperator> list) {
		HashSet<Variable> result = new HashSet<Variable>();

		for (BasicOperator op : list) {
			for (Variable var : op.getUnionVariables()) {
				result.add(var);
			}
		}
		return new ArrayList<Variable>(result);
	}

	public static void addProjectionFromMultiInputOperatorInContainerIfNecessary(
			BasicOperator multiInputOperator, Set<BasicOperator> containerList) {
		ArrayList<Variable> intersectionVariables = new ArrayList<Variable>(
				multiInputOperator.getIntersectionVariables());
		if (intersectionVariables.size() > 0) {
			Projection proj = new Projection();
			for (Variable var : intersectionVariables) {
				proj.addProjectionElement(var);
			}
			for (BasicOperator indexScan : containerList) {
				if (indexScan instanceof IndexScanContainer) {
					((IndexScanContainer) indexScan).addOperator(proj);
				} else {
					((MultiIndexScanContainer) indexScan)
							.addOperatorToAllChilds(proj);
				}
			}
		}

	}

	public static void addProjectionIfNecessary(BasicOperator operation,
			ArrayList<BasicOperator> containerList) {
		ArrayList<Variable> intersectionVariables = new ArrayList<Variable>(
				operation.getIntersectionVariables());

		// Beim OrderBy Operator umfassen die IntersectionVariablen alle
		// Variablen des Triple Pattern, es ist aber nur nötig die OrderBy
		// Variable zu pushen
		if (operation instanceof Sort) {
			intersectionVariables = new ArrayList<Variable>(
					((Sort) operation).getSortCriterium());
		}

		if (intersectionVariables.size() > 0) {
			Projection proj = new Projection();
			for (Variable var : intersectionVariables) {
				proj.addProjectionElement(var);
			}
			for (BasicOperator indexScan : containerList) {
				if (indexScan instanceof IndexScanContainer) {
					((IndexScanContainer) indexScan).addOperator(proj);
				} else {
					((MultiIndexScanContainer) indexScan)
							.addOperatorToAllChilds(proj);
				}
			}
		}

	}

	public static void insertNewOperator(BasicOperator existingOperation,
			BasicOperator newOperation) {
		final List<OperatorIDTuple> succs = existingOperation
				.getSucceedingOperators();

		for (OperatorIDTuple succ : succs) {
			existingOperation.removeSucceedingOperator(succ);
			newOperation.addSucceedingOperator(succ);
			succ.getOperator().removePrecedingOperator(existingOperation);
			succ.getOperator().addPrecedingOperator(newOperation);
		}

		existingOperation.addSucceedingOperator(newOperation);
		newOperation.addPrecedingOperator(existingOperation);
	}

	public static void mergeContainerListIntoOneNewContainer(
			BasicOperator newContainer, HashSet<BasicOperator> oldContainer) {
		// Merke alte Vorgänger und Nachfolger des Containers
		HashSet<BasicOperator> preds = new HashSet<BasicOperator>();
		HashSet<BasicOperator> succs = new HashSet<BasicOperator>();

		for (BasicOperator container : oldContainer) {
			// merken
			preds.addAll(container.getPrecedingOperators());
			for (OperatorIDTuple idtuple : container.getSucceedingOperators()) {
				succs.add(idtuple.getOperator());
			}

			// löschen
			// container.removeFromOperatorGraph();
			container.setSucceedingOperators(new LinkedList<OperatorIDTuple>());
			container.setPrecedingOperators(new LinkedList<BasicOperator>());

		}

		// Für jeden Vorgänger den neuen Container setzen und die alten löschen
		for (BasicOperator prec : preds) {
			prec.addSucceedingOperator(newContainer);
			for (BasicOperator container : oldContainer) {
				prec.removeSucceedingOperator(container);
			}
		}

		// Für die neue Operation die alten Vorgänger setzen
		newContainer
				.setPrecedingOperators(new LinkedList<BasicOperator>(preds));

		// Für jeden Nachfolger den neuen Vorgänger setzen
		for (BasicOperator succ : succs) {
			succ.addPrecedingOperator(newContainer);
			for (BasicOperator container : oldContainer) {
				succ.removePrecedingOperator(container);
			}
		}

		// Für die neue Operation die alten Nachfolger
		for (BasicOperator toAdd : succs) {
			newContainer.addSucceedingOperator(toAdd);
		}
	}

	public static void removeDuplicatedEdges(BasicOperator op) {
		HashSet<BasicOperator> preds = new HashSet<BasicOperator>(
				op.getPrecedingOperators());
		HashSet<OperatorIDTuple> succs = new HashSet<OperatorIDTuple>(
				op.getSucceedingOperators());

		op.setPrecedingOperators(new ArrayList<BasicOperator>(preds));
		op.setSucceedingOperators(new ArrayList<OperatorIDTuple>(succs));

	}

	public static BasicOperator getLastOperatorOfContainer(
			BasicOperator operator) {
		BasicOperator result = null;
		if (operator.getSucceedingOperators() == null
				|| operator.getSucceedingOperators().size() == 0) {
			result = operator;
		} else {
			for (OperatorIDTuple elem : operator.getSucceedingOperators()) {
				result = getLastOperatorOfContainer(elem.getOperator());
			}
		}
		return result;
	}
}
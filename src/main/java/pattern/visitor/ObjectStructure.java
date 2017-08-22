package pattern.visitor;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by fengwu
 * Date: 2017/8/21
 */
public class ObjectStructure {
    private List<ElementNode> nodeList = new ArrayList<>();

    public void action(Visitor vistor) {
        for (ElementNode node : nodeList) {
            node.accept(vistor);
        }
    }

    public void add(ElementNode node) {
        nodeList.add(node);
    }
}

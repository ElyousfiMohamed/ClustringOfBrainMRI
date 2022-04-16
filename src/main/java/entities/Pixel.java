package entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Pixel {
    private int x, y, color;

    public double distance(Pixel p) {
        return Math.sqrt(Math.pow((p.getX() - this.getX()), 2) + Math.pow((p.getY() - this.getY()), 2));
    }

    @Override
    public String toString() {
        return x + " " + y + " " + color;
    }
}
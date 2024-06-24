import { Feature } from './Feature';

export class Layer {
    name: string;
    version: number;
    length: number;
    features: Feature[];

    constructor(name: string, version : number, features: Feature[]) {
        this.name = name;
        this.version = version;
        this.length = features.length;
        this.features = features;
    }

    public feature(i: number): Feature {
        if (i < 0 || i >= this.features.length) throw new Error('feature index out of bounds');
        return this.features[i];
    }
